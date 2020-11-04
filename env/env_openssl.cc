//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
//  env_encryption.cc copied to this file then modified.

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <algorithm>
#include <cctype>
#include <iostream>
#include <mutex>

#include "env/env_openssl_impl.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

static std::once_flag crypto_loaded;
static std::shared_ptr<UnixLibCrypto> crypto_shared;

std::shared_ptr<UnixLibCrypto> GetCrypto() {
  std::call_once(crypto_loaded,
                 []() { crypto_shared = std::make_shared<UnixLibCrypto>(); });
  return crypto_shared;
}

// reuse cipher context between calls to Encrypt & Decrypt
static void do_nothing(EVP_CIPHER_CTX*){};
thread_local static std::unique_ptr<EVP_CIPHER_CTX, void (*)(EVP_CIPHER_CTX*)>
    aes_context(nullptr, &do_nothing);

ShaDescription::ShaDescription(const std::string& key_desc_str) {
  GetCrypto();  // ensure libcryto available
  bool good = {true};
  int ret_val;
  unsigned len;

  memset(desc, 0, EVP_MAX_MD_SIZE);
  if (0 != key_desc_str.length() && crypto_shared->IsValid()) {
    std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX*)> context(
        crypto_shared->EVP_MD_CTX_new(), crypto_shared->EVP_MD_CTX_free_ptr());

    ret_val = crypto_shared->EVP_DigestInit_ex(
        context.get(), crypto_shared->EVP_sha1(), nullptr);
    good = (1 == ret_val);
    if (good) {
      ret_val = crypto_shared->EVP_DigestUpdate(
          context.get(), key_desc_str.c_str(), key_desc_str.length());
      good = (1 == ret_val);
    }

    if (good) {
      ret_val = crypto_shared->EVP_DigestFinal_ex(context.get(), desc, &len);
      good = (1 == ret_val);
    }
  } else {
    good = false;
  }

  valid = good;
}

std::shared_ptr<ShaDescription> NewShaDescription(
    const std::string& key_desc_str) {
  return std::make_shared<ShaDescription>(key_desc_str);
}

AesCtrKey::AesCtrKey(const std::string& key_str) : valid(false) {
  GetCrypto();  // ensure libcryto available
  memset(key, 0, EVP_MAX_KEY_LENGTH);

  // simple parse:  must be 64 characters long and hexadecimal values
  if (64 == key_str.length()) {
    auto bad_pos = key_str.find_first_not_of("abcdefABCDEF0123456789");
    if (std::string::npos == bad_pos) {
      for (size_t idx = 0, idx2 = 0; idx < key_str.length(); idx += 2, ++idx2) {
        std::string hex_string(key_str.substr(idx, 2));
        key[idx2] = std::stoul(hex_string, 0, 16);
      }
      valid = true;
    }
  }
}

// code tests for 64 character hex string to yield 32 byte binary key
std::shared_ptr<AesCtrKey> NewAesCtrKey(const std::string& hex_key_str) {
  return std::make_shared<AesCtrKey>(hex_key_str);
}

void AESBlockAccessCipherStream::BigEndianAdd128(uint8_t* buf, uint64_t value) {
  uint8_t *sum, *addend, *carry, pre, post;

  sum = buf + 15;

  if (port::kLittleEndian) {
    addend = (uint8_t*)&value;
  } else {
    addend = (uint8_t*)&value + 7;
  }

  // future:  big endian could be written as uint64_t add
  for (int loop = 0; loop < 8 && value; ++loop) {
    pre = *sum;
    *sum += *addend;
    post = *sum;
    --sum;
    value >>= 8;

    carry = sum;
    // carry?
    while (post < pre && buf <= carry) {
      pre = *carry;
      *carry += 1;
      post = *carry;
      --carry;
    }
  }  // for
}

// "data" is assumed to be aligned at AES_BLOCK_SIZE or greater
Status AESBlockAccessCipherStream::Encrypt(uint64_t file_offset, char* data,
                                           size_t data_size) {
  Status status;
  if (0 < data_size) {
    if (crypto_shared->IsValid()) {
      int ret_val, out_len;
      ALIGN16 uint8_t iv[AES_BLOCK_SIZE];
      ALIGN16 uint8_t local_data[AES_BLOCK_SIZE];
      uint64_t block_index = file_offset / BlockSize();
      uint64_t data_offset = file_offset % BlockSize();
      uint64_t partial_size;

      if (data_offset) {
        partial_size = BlockSize() - data_offset;
      } else {
        partial_size = 0;
      }

      // make a context once per thread
      if (!aes_context) {
        aes_context =
            std::unique_ptr<EVP_CIPHER_CTX, void (*)(EVP_CIPHER_CTX*)>(
                crypto_shared->EVP_CIPHER_CTX_new(),
                crypto_shared->EVP_CIPHER_CTX_free_ptr());
      }

      memcpy(iv, nonce_, AES_BLOCK_SIZE);
      BigEndianAdd128(iv, block_index);

      ret_val = crypto_shared->EVP_EncryptInit_ex(
          aes_context.get(), crypto_shared->EVP_aes_256_ctr(), nullptr,
          key_.key, iv);
      if (1 == ret_val) {
        // do partial block via local storage and xor
        if (0 != data_offset) {
          memset(local_data, 0, AES_BLOCK_SIZE);
          out_len = 0;
          ret_val = crypto_shared->EVP_EncryptUpdate(
              aes_context.get(), (unsigned char*)local_data, &out_len,
              (unsigned char*)local_data, (int)AES_BLOCK_SIZE);
          if (partial_size < data_size) {
            data_size -= partial_size;
          } else {
            partial_size = data_size;
            data_size = 0;
          }

          if (1 == ret_val && AES_BLOCK_SIZE == out_len) {
            for (uint64_t loop = 0; loop < partial_size; ++loop) {
              data[loop] ^= local_data[loop + data_offset];
            }
          } else {
            status = Status::InvalidArgument(
                "EVP_EncryptUpdate failed 1: ",
                0 == ret_val ? "bad return value" : "output length short");
          }
        }

        // do remaining, aligned segment
        if (status.ok() && data_size) {
          out_len = 0;
          ret_val = crypto_shared->EVP_EncryptUpdate(
              aes_context.get(), (unsigned char*)(data + partial_size),
              &out_len, (unsigned char*)(data + partial_size), (int)data_size);

          if (1 != ret_val || (int)data_size != out_len) {
            status = Status::InvalidArgument(
                "EVP_EncryptUpdate failed 2: ",
                0 == ret_val ? "bad return value" : "output length short");
          }
        }

        if (status.ok()) {
          // this is a soft reset of aes_context per man pages
          out_len = 0;
          ret_val = crypto_shared->EVP_EncryptFinal_ex(aes_context.get(),
                                                       local_data, &out_len);

          if (1 != ret_val || 0 != out_len) {
            status = Status::InvalidArgument(
                "EVP_EncryptFinal_ex failed: ",
                (1 != ret_val) ? "bad return value" : "output length short");
          }
        }
      } else {
        status = Status::InvalidArgument("EVP_EncryptInit_ex failed.");
      }
    } else {
      status = Status::NotSupported(
          "libcrypto not available for encryption/decryption.");
    }
  }

  return status;
}

// Decrypt one or more (partial) blocks of data at the file offset.
//  Length of data is given in data_size.
//  CTR Encrypt and Decrypt are synonyms.  Using Encrypt calls here to reduce
//   count of symbols loaded from libcrypto.
Status AESBlockAccessCipherStream::Decrypt(uint64_t file_offset, char* data,
                                           size_t data_size) {
  return Encrypt(file_offset, data, data_size);
}

Status OpenSSLEncryptionProvider::CreateNewPrefix(const std::string& /*fname*/,
                                                  char* prefix,
                                                  size_t prefixLength) const {
  GetCrypto();  // ensure libcryto available
  Status s;
  if (crypto_shared->IsValid()) {
    if ((sizeof(EncryptMarker) + sizeof(PrefixVersion0)) <= prefixLength) {
      int ret_val;

      PrefixVersion0* pf = (PrefixVersion0*)(prefix + sizeof(EncryptMarker));
      memcpy(prefix, kEncryptMarker, sizeof(kEncryptMarker));
      *(prefix + 7) = kEncryptCodeVersion1;
      memcpy(pf->key_description_, encrypt_write_.first.desc,
             sizeof(ShaDescription::desc));
      ret_val = crypto_shared->RAND_bytes((unsigned char*)&pf->nonce_,
                                          AES_BLOCK_SIZE);
      if (1 != ret_val) {
        s = Status::NotSupported("RAND_bytes failed");
      }
    } else {
      s = Status::NotSupported("Prefix size needs to be 28 or more");
    }
  } else {
    s = Status::NotSupported("RAND_bytes() from libcrypto not available.");
  }

  return s;
}

Status OpenSSLEncryptionProvider::AddCipher(const std::string& descriptor,
                                            const char* cipher, size_t len,
                                            bool for_write) {
  Status s;
  std::string hex_cipher(cipher, len);
  AesCtrKey key(hex_cipher);
  ShaDescription desc(descriptor);

  if (key.IsValid() && desc.IsValid()) {
    if (for_write) {
      encrypt_write_ = std::pair<ShaDescription, AesCtrKey>(desc, key);
    }

    auto ret =
        encrypt_read_.insert(std::pair<ShaDescription, AesCtrKey>(desc, key));
    if (!ret.second) {
      s = Status::InvalidArgument("Duplicate descriptor / cipher pair");
    }
  } else {
    s = Status::InvalidArgument("Bad descriptor / cipher pair");
  }

  return s;
}

Status OpenSSLEncryptionProvider::CreateCipherStream(
    const std::string& /*fname*/, const EnvOptions& /*options*/, Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  Status status;

  if ((sizeof(EncryptMarker) + sizeof(PrefixVersion0)) <= prefix.size() &&
      prefix.starts_with(kEncryptMarker)) {
    uint8_t code_version = (uint8_t)prefix[7];

    if (kEncryptCodeVersion1 == code_version) {
      Slice prefix_slice;
      PrefixVersion0* prefix_buffer =
          (PrefixVersion0*)(prefix.data() + sizeof(EncryptMarker));
      ShaDescription desc(prefix_buffer->key_description_,
                          sizeof(PrefixVersion0::key_description_));

      ReadLock lock(&key_lock_);
      auto it = encrypt_read_.find(desc);
      if (encrypt_read_.end() != it) {
        result->reset(new AESBlockAccessCipherStream(it->second, code_version,
                                                     prefix_buffer->nonce_));
      } else {
        status =
            Status::NotSupported("No encryption key found to match input file");
      }
    } else {
      status =
          Status::NotSupported("Unknown encryption code version required.");
    }
  } else {
    status = Status::EncryptionUnknown(
        "Unknown encryption marker or not encrypted.");
  }

  return status;
}

std::string OpenSSLEncryptionProvider::GetMarker() const {
  return kEncryptMarker;
}

Status NewOpenSSLEncryptionProvider(
    std::shared_ptr<EncryptionProvider>* result) {
  Status stat;
  result->reset();

  // is library available?
  std::shared_ptr<UnixLibCrypto> crypto = GetCrypto();
  if (crypto) {
    std::shared_ptr<OpenSSLEncryptionProvider> temp(
        std::make_shared<OpenSSLEncryptionProvider>());
    result->operator=(std::static_pointer_cast<EncryptionProvider>(temp));
  } else {
    stat = Status::NotSupported(
        "libcrypto not available for encryption/decryption.");
  }
  return stat;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
