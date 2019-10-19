//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"

namespace rocksdb {

class ShouldStopBefore {  
public:
  ShouldStopBefore() = delete;
  
  ShouldStopBefore(const InternalKeyComparator * icmp, const std::vector<FileMetaData*>& grandparents,
                   uint64_t max_compaction_bytes, const std::shared_ptr<const SliceTransform>& cpe,
                   bool prefix_strict);
  
  bool test_key(const Slice& internal_key, uint64_t curr_file_size);

protected:
  const InternalKeyComparator* icmp ;
  const std::vector<FileMetaData*>& grandparents;
  
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;
  // from compaction->max_compaction_bytes()
  uint64_t max_compaction_bytes = 0;

  const std::shared_ptr<const SliceTransform> compaction_prefix_extractor;
  bool compaction_prefix_strict = false;
  
};// class ShouldStopBefore

} // namespece rocksdb
