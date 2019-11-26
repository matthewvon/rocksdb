//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "db/error_handler.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/writable_file_writer.h"
#include "logging/event_logger.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/table_builder.h"

namespace rocksdb {

struct OutputFilesState {
  ColumnFamilyData* cfd;
  
  // The boundaries of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  Slice *start, *end;

  // The return status of this subcompaction
  Status status;

  // Files produced by this subcompaction
  struct Output {
    FileMetaData meta;
    bool finished;
    std::shared_ptr<const TableProperties> table_properties;
  };

  // State kept for output being generated
  std::vector<Output> outputs;
  std::unique_ptr<WritableFileWriter> outfile;
  std::unique_ptr<TableBuilder> builder;
  Output* current_output() {
    if (outputs.empty()) {
      // This subcompaction's outptut could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particulalry likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }

  EventLogger* event_logger;
  const std::string& dbname;
  int job_id;
  bool is_flush;
  bool bottommost_level;
  InstrumentedMutex* db_mutex;
  SequenceNumber earliest_snapshot;
  ErrorHandler* db_error_handler;
  VersionSet * versions;
  uint32_t output_path_id;
  uint64_t preallocation_size;
  Env::WriteLifeTimeHint write_hint;
  uint64_t max_input_file_creation_time;
  TableBuilderOptions tboptions;  // local copy so last four members can change
  
  uint64_t current_output_file_size;

  // State during the subcompaction
  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;
  CompactionJobStats compaction_job_stats;
  uint64_t approx_size;  /* this appears unused in original compaction_job.cc code */

  
OutputFilesState(ColumnFamilyData * _cfd, Slice* _start, Slice* _end,
                 EventLogger * _event_logger, const std::string& _dbname,
                 int _job_id, bool _is_flush, bool _bottommost_level,
                 InstrumentedMutex* _db_mutex, SequenceNumber _earliest_snapshot,
                 ErrorHandler* _db_error_handler, VersionSet * _versions,
                 uint32_t _output_path_id, TableBuilderOptions _tboptions, uint64_t size = 0)
  : cfd(_cfd),
    start(_start),
    end(_end),
    outfile(nullptr),
    builder(nullptr),
    event_logger(_event_logger),
    dbname(_dbname),
    job_id(_job_id),
    is_flush(_is_flush),
    bottommost_level(_bottommost_level),
    db_mutex(_db_mutex),
    earliest_snapshot(_earliest_snapshot),
    db_error_handler(_db_error_handler),
    versions(_versions),
    output_path_id(_output_path_id),
    tboptions(_tboptions),
    current_output_file_size(0),
    total_bytes(0),
    num_input_records(0),
    num_output_records(0),
    approx_size(size) {
    }

  OutputFilesState(OutputFilesState&& o)
  : dbname(std::move(o.dbname)), tboptions(o.tboptions)// {/* *this = std::move(o);*/ }

//  OutputFilesState& operator=(OutputFilesState&& o) {
  {
    status = std::move(o.status);
    cfd = std::move(o.cfd);
    start = std::move(o.start);
    end = std::move(o.end);
    outputs = std::move(o.outputs);
    outfile = std::move(o.outfile);
    builder = std::move(o.builder);
    event_logger = std::move(o.event_logger);
    job_id = std::move(o.job_id);
    is_flush = std::move(o.is_flush);
    bottommost_level = std::move(o.bottommost_level);
    db_mutex = std::move(o.db_mutex);
    earliest_snapshot = std::move(o.earliest_snapshot);
    db_error_handler = std::move(o.db_error_handler);
    versions = std::move(o.versions);
    output_path_id = std::move(o.output_path_id);
    preallocation_size = std::move(o.preallocation_size);
    write_hint = std::move(write_hint);
    max_input_file_creation_time = std::move(max_input_file_creation_time);
    current_output_file_size = std::move(o.current_output_file_size);
    total_bytes = std::move(o.total_bytes);
    num_input_records = std::move(o.num_input_records);
    num_output_records = std::move(o.num_output_records);
    compaction_job_stats = std::move(o.compaction_job_stats);
    approx_size = std::move(o.approx_size);
//    return *this;
  }    

  // Because member std::unique_ptrs do not have these.
  OutputFilesState(const OutputFilesState&) = delete;

  OutputFilesState& operator=(const OutputFilesState&) = delete;
    

  Status OpenCompactionOutputFile();

  Status FinishCompactionOutputFile(
    const Status& input_status,
    CompactionRangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const Slice* next_table_min_key = nullptr);

};

}  // namespace rocksdb
