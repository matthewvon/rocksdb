//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/should_stop_before.h"

namespace rocksdb {

ShouldStopBefore::ShouldStopBefore(const InternalKeyComparator * Icmp,
                                   const std::vector<FileMetaData*>& Grandparents,
                                   uint64_t MaxCompactionBytes,
                                   const std::shared_ptr<const SliceTransform>& cpe,
                                   bool prefix_strict)
  : icmp(Icmp), grandparents(Grandparents),
    grandparent_index(0), overlapped_bytes(0), seen_key(false),
    max_compaction_bytes(MaxCompactionBytes),
    compaction_prefix_extractor(cpe), compaction_prefix_strict(prefix_strict) {};
  
  
// Returns true iff we should stop building the current output
// before processing "internal_key".
bool ShouldStopBefore::test_key(const Slice& internal_key, uint64_t curr_file_size) {

  // Scan to find earliest grandparent file that contains key.
  while (grandparent_index < grandparents.size() &&
         icmp->Compare(internal_key,
                       grandparents[grandparent_index]->largest.Encode()) >
         0) {
    if (seen_key) {
      overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
    }
    assert(grandparent_index + 1 >= grandparents.size() ||
           icmp->Compare(
             grandparents[grandparent_index]->largest.Encode(),
             grandparents[grandparent_index + 1]->smallest.Encode()) <= 0);
    grandparent_index++;
  }
  seen_key = true;
  
  if (overlapped_bytes + curr_file_size > max_compaction_bytes) {
    // Too much overlap for current output; start new output
    overlapped_bytes = 0;
    return true;
  }
  
  return false;
} // ShouldStopBefore::test_key
  
} // namespace rocksdb

