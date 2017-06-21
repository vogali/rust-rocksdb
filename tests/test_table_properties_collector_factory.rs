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

use rocksdb::{Writable, DB, TablePropertiesCollector,TablePropertiesCollectorFactory, Options};
use tempdir::TempDir;
use std::collections::HashMap;
use rocksdb::crocksdb_ffi::EntryType;


struct CollectorFactory {
    _cnt: u64,
}

struct Collector {
    cnt: u32,
}

impl TablePropertiesCollector for Collector {
    fn add_userkey(&mut self, key: &[u8], value: &[u8], entry_type: EntryType){
        println!("add user key test");
        if key[0]==b'A'{
            self.cnt=self.cnt+1;
        }
    }
    fn finish(&mut self) -> HashMap<String, String> {
        let mut user_properties: HashMap<String, String> = HashMap::new();
        user_properties.insert(String::from("The number of keys started with A"), 
            format!("{}", self.cnt));
        user_properties 
    }
}

impl TablePropertiesCollectorFactory for CollectorFactory{
    fn create_table_properties_collector(&mut self, context: u32) 
                                        -> Box<TablePropertiesCollector>
    {
        Box::new(Collector{cnt:0})
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        println!("Drop collector");
    }
}

impl Drop for CollectorFactory {
    fn drop(&mut self) {
        println!("Drop collector factory");
    }
}

#[test]
fn test_table_properties_collector_factory() {
    let path = TempDir::new("_rust_rocksdb_collectortest").expect("");
    let mut opts = Options::new();
    opts.add_table_properties_collector_factory("test",
                               Box::new(CollectorFactory {
                                   _cnt: 0,
                               }))
        .unwrap();
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
    let samples = vec![(b"key1".to_vec(), b"value1".to_vec()),
                       (b"key2".to_vec(), b"value2".to_vec())];
    for &(ref k, ref v) in &samples {
        db.put(k, v).unwrap();
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
    db.flush(true).unwrap();
    drop(db);

    
}
