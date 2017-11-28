// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::*;
use tempdir::TempDir;
use test_ingest_external_file::gen_sst;

#[test]
fn test_sst_file_reader() {
    let path = TempDir::new("_rust_rocksdb_temp_sst_file").expect("");
    let sstfile_str = path.path().to_str().unwrap();
    gen_sst(
        ColumnFamilyOptions::new(),
        None,
        sstfile_str,
        &[(b"k1", b"v1"), (b"k2", b"v2")],
    );

    let mut reader = SstFileReader(sstfile_str.as_bytes(), 0);
    let props = reader.get_properties();
    assert_eq!(props.raw_key_size(), 2);
    assert_eq!(props.raw_value_size(), 2);
    let user_props = props.user_collected_properties();
    let seqno_str = user_props
        .get(b"rocksdb.external_sst_file.global_seqno")
        .unwrap();
    assert_eq!(seqno_str, b"0");
    assert!(props.get_property_offset(b"rocksdb.external_sst_file.global_seqno"));
}
