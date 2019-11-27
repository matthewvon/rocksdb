//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <inttypes.h>

#include "db/column_family.h"
#include "db/event_helpers.h"
#include "db/builder.h"
#include "db/output_files_state.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"

namespace rocksdb {

Status OutputFilesState::OpenCompactionOutputFile() {
  assert(builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions->NewFileNumber();
  std::string fname =
    TableFileName(cfd->ioptions()->cf_paths,
                    file_number, output_path_id);
  // Fire events.
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname, cfd->GetName(), fname, job_id,
      (is_flush ? TableFileCreationReason::kFlush : TableFileCreationReason::kCompaction));
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<WritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = cfd->soptions()->use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif
  Status s = NewWritableFile(cfd->ioptions()->env, fname, &writable_file, *cfd->soptions());
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        cfd->ioptions()->info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        cfd->GetName().c_str(),
        job_id, file_number, s.ToString().c_str());
    LogFlush(cfd->ioptions()->info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger, cfd->ioptions()->listeners, dbname, cfd->GetName(),
        fname, job_id, FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  Output out;
  out.meta.fd =
      FileDescriptor(file_number, output_path_id, 0);
  out.finished = false;

  outputs.push_back(out);
  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(write_hint);
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(preallocation_size));
  const auto& listeners =
    cfd->ioptions()->listeners;
  outfile.reset(
    new WritableFileWriter(std::move(writable_file), fname, *env_options, //*cfd->soptions(),
                             cfd->ioptions()->env, cfd->ioptions()->statistics, listeners));

  int64_t temp_current_time = 0;
  auto get_time_status = cfd->ioptions()->env->GetCurrentTime(&temp_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!get_time_status.ok()) {
    ROCKS_LOG_WARN(cfd->ioptions()->info_log,
                   "Failed to get current time. Status: %s",
                   get_time_status.ToString().c_str());
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);

  uint64_t latest_key_time = max_input_file_creation_time;
  if (latest_key_time == 0) {
    latest_key_time = current_time;
  }

  tboptions.creation_time = latest_key_time;
  tboptions.file_creation_time = current_time;
  builder.reset(NewTableBuilder(tboptions,
                                cfd->GetID(), outfile.get()));
  LogFlush(cfd->ioptions()->info_log);
  return s;
} // OutputFilesState::OpenCompactionOutputFile

  Status OutputFilesState::FinishCompactionOutputFile(
    const Status& input_status, 
    CompactionRangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const Slice* next_table_min_key /* = nullptr */) {

  uint64_t output_number = current_output()->meta.fd.GetNumber();
  assert(output_number != 0);

  const Comparator* ucmp = cfd->user_comparator();

  // Check for iterator errors
  Status s = input_status;
  auto meta = &current_output()->meta;
  assert(meta != nullptr);
  if (s.ok()) {
    Slice lower_bound_guard, upper_bound_guard;
    std::string smallest_user_key;
    const Slice *lower_bound, *upper_bound;
    bool lower_bound_from_sub_compact = false;
    if (outputs.size() == 1) {
      // For the first output table, include range tombstones before the min key
      // but after the subcompaction boundary.
      lower_bound = start;
      lower_bound_from_sub_compact = true;
    } else if (meta->smallest.size() > 0) {
      // For subsequent output tables, only include range tombstones from min
      // key onwards since the previous file was extended to contain range
      // tombstones falling before min key.
      smallest_user_key = meta->smallest.user_key().ToString(false /*hex*/);
      lower_bound_guard = Slice(smallest_user_key);
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
    if (next_table_min_key != nullptr) {
      // This may be the last file in the subcompaction in some cases, so we
      // need to compare the end key of subcompaction with the next file start
      // key. When the end key is chosen by the subcompaction, we know that
      // it must be the biggest key in output file. Therefore, it is safe to
      // use the smaller key as the upper bound of the output file, to ensure
      // that there is no overlapping between different output files.
      upper_bound_guard = ExtractUserKey(*next_table_min_key);
      if (end != nullptr &&
          ucmp->Compare(upper_bound_guard, *end) >= 0) {
        upper_bound = end;
      } else {
        upper_bound = &upper_bound_guard;
      }
    } else {
      // This is the last file in the subcompaction, so extend until the
      // subcompaction ends.
      upper_bound = end;
    }
#if 0
    auto earliest_snapshot = kMaxSequenceNumber;
    if (existing_snapshots_.size() > 0) {
      earliest_snapshot = existing_snapshots_[0];
    }
#endif    
    bool has_overlapping_endpoints;
    if (upper_bound != nullptr && meta->largest.size() > 0) {
      has_overlapping_endpoints =
          ucmp->Compare(meta->largest.user_key(), *upper_bound) == 0;
    } else {
      has_overlapping_endpoints = false;
    }

    // The end key of the subcompaction must be bigger or equal to the upper
    // bound. If the end of subcompaction is null or the upper bound is null,
    // it means that this file is the last file in the compaction. So there
    // will be no overlapping between this file and others.
    assert(end == nullptr ||
           upper_bound == nullptr ||
           ucmp->Compare(*upper_bound , *end) <= 0);
    auto it = range_del_agg->NewIterator(lower_bound, upper_bound,
                                         has_overlapping_endpoints);
    // Position the range tombstone output iterator. There may be tombstone
    // fragments that are entirely out of range, so make sure that we do not
    // include those.
    if (lower_bound != nullptr) {
      it->Seek(*lower_bound);
    } else {
      it->SeekToFirst();
    }
    for (; it->Valid(); it->Next()) {
      auto tombstone = it->Tombstone();
      if (upper_bound != nullptr) {
        int cmp = ucmp->Compare(*upper_bound, tombstone.start_key_);
        if ((has_overlapping_endpoints && cmp < 0) ||
            (!has_overlapping_endpoints && cmp <= 0)) {
          // Tombstones starting after upper_bound only need to be included in
          // the next table. If the current SST ends before upper_bound, i.e.,
          // `has_overlapping_endpoints == false`, we can also skip over range
          // tombstones that start exactly at upper_bound. Such range tombstones
          // will be included in the next file and are not relevant to the point
          // keys or endpoints of the current file.
          break;
        }
      }

      if (bottommost_level && tombstone.seq_ <= earliest_snapshot) {
        // TODO(andrewkr): tombstones that span multiple output files are
        // counted for each compaction output file, so lots of double counting.
        range_del_out_stats->num_range_del_drop_obsolete++;
        range_del_out_stats->num_record_drop_obsolete++;
        continue;
      }

      auto kv = tombstone.Serialize();
      assert(lower_bound == nullptr ||
             ucmp->Compare(*lower_bound, kv.second) < 0);
      builder->Add(kv.first.Encode(), kv.second);
      InternalKey smallest_candidate = std::move(kv.first);
      if (lower_bound != nullptr &&
          ucmp->Compare(smallest_candidate.user_key(), *lower_bound) <= 0) {
        // Pretend the smallest key has the same user key as lower_bound
        // (the max key in the previous table or subcompaction) in order for
        // files to appear key-space partitioned.
        //
        // When lower_bound is chosen by a subcompaction, we know that
        // subcompactions over smaller keys cannot contain any keys at
        // lower_bound. We also know that smaller subcompactions exist, because
        // otherwise the subcompaction woud be unbounded on the left. As a
        // result, we know that no other files on the output level will contain
        // actual keys at lower_bound (an output file may have a largest key of
        // lower_bound@kMaxSequenceNumber, but this only indicates a large range
        // tombstone was truncated). Therefore, it is safe to use the
        // tombstone's sequence number, to ensure that keys at lower_bound at
        // lower levels are covered by truncated tombstones.
        //
        // If lower_bound was chosen by the smallest data key in the file,
        // choose lowest seqnum so this file's smallest internal key comes after
        // the previous file's largest. The fake seqnum is OK because the read
        // path's file-picking code only considers user key.
        smallest_candidate = InternalKey(
            *lower_bound, lower_bound_from_sub_compact ? tombstone.seq_ : 0,
            kTypeRangeDeletion);
      }
      InternalKey largest_candidate = tombstone.SerializeEndKey();
      if (upper_bound != nullptr &&
          ucmp->Compare(*upper_bound, largest_candidate.user_key()) <= 0) {
        // Pretend the largest key has the same user key as upper_bound (the
        // min key in the following table or subcompaction) in order for files
        // to appear key-space partitioned.
        //
        // Choose highest seqnum so this file's largest internal key comes
        // before the next file's/subcompaction's smallest. The fake seqnum is
        // OK because the read path's file-picking code only considers the user
        // key portion.
        //
        // Note Seek() also creates InternalKey with (user_key,
        // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
        // kTypeRangeDeletion (0xF), so the range tombstone comes before the
        // Seek() key in InternalKey's ordering. So Seek() will look in the
        // next file for the user key.
        largest_candidate =
            InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
      }
#ifndef NDEBUG
      SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
      if (meta->smallest.size() > 0) {
        smallest_ikey_seqnum = GetInternalKeySeqno(meta->smallest.Encode());
      }
#endif
      meta->UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                     tombstone.seq_,
                                     cfd->internal_comparator());

      // The smallest key in a file is used for range tombstone truncation, so
      // it cannot have a seqnum of 0 (unless the smallest data key in a file
      // has a seqnum of 0). Otherwise, the truncated tombstone may expose
      // deleted keys at lower levels.
      assert(smallest_ikey_seqnum == 0 ||
             ExtractInternalKeyFooter(meta->smallest.Encode()) !=
                 PackSequenceAndType(0, kTypeRangeDeletion));
    }
    meta->marked_for_compaction = builder->NeedCompact();
  }
  const uint64_t current_entries = builder->NumEntries();
  if (s.ok()) {
    s = builder->Finish();
  } else {
    builder->Abandon();
  }
  const uint64_t current_bytes = builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
  }
  current_output()->finished = true;
  total_bytes += current_bytes;

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(cfd->ioptions()->env, cfd->ioptions()->statistics, COMPACTION_OUTFILE_SYNC_MICROS);
    s = outfile->Sync(cfd->ioptions()->use_fsync);
  }
  if (s.ok()) {
    s = outfile->Close();
  }
  outfile.reset();

  TableProperties tp;
  if (s.ok()) {
    tp = builder->GetTableProperties();
  }

  if (s.ok() && current_entries == 0 && tp.num_range_deletions == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname =
        TableFileName(cfd->ioptions()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    cfd->ioptions()->env->DeleteFile(fname);

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    assert(!outputs.empty());
    outputs.pop_back();
    meta = nullptr;
  }

  if (s.ok() && (current_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    current_output()->table_properties =
        std::make_shared<TableProperties>(tp);
    ROCKS_LOG_INFO(cfd->ioptions()->info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id, output_number,
                   current_entries, current_bytes,
                   meta->marked_for_compaction ? " (need compaction)" : "");
  }
  std::string fname;
  FileDescriptor output_fd;
  if (meta != nullptr) {
    fname =
        TableFileName(cfd->ioptions()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    output_fd = meta->fd;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, cfd->ioptions()->listeners, dbname, cfd->GetName(), fname,
      job_id, output_fd, tp,
      (is_flush ? TableFileCreationReason::kFlush : TableFileCreationReason::kCompaction), s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(cfd->ioptions()->sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    sfm->OnAddFile(fname);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex);
      db_error_handler->SetBGError(s,
                                    (is_flush ? BackgroundErrorReason::kFlush
                                     : BackgroundErrorReason::kCompaction));
    }
  }
#endif

  builder.reset();
  current_output_file_size = 0;
  return s;
}

} // namespace rocksdb

