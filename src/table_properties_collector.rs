use crocksdb_ffi::{self, DBTablePropertiesCollector,EntryType};
use libc::{c_void, c_char, c_int, size_t};
use std::ffi::CString;
use std::slice;
use std::mem;
use std::mem::size_of;
use std::collections::HashMap;
use std::ptr;
use libc::malloc;

pub trait TablePropertiesCollector {
    fn add_userkey(&mut self, key: &[u8], value: &[u8], entry_type: EntryType);
    fn finish(&mut self) -> HashMap<String, String>;
}



#[repr(C)]
pub struct TablePropertiesCollectorProxy {
    name: CString,
    collector: Box<TablePropertiesCollector>,
}



pub extern "C" fn name(collector: *mut c_void) -> *const c_char {
    unsafe { 
        (*(collector as *mut TablePropertiesCollectorProxy)).name.as_ptr() 
    }
}

pub extern "C" fn destructor(collector: *mut c_void) {
    unsafe {
        Box::from_raw(collector as *mut TablePropertiesCollectorProxy);
    }
}

pub extern "C" fn add_userkey(collector: *mut c_void,
                     key: *const u8,
                     key_length: size_t,
                     value: *const u8,
                     value_length: size_t,
                     entry_type: c_int,
                     _: u64,
                     _: u64) {
    unsafe {
        panic!("add_userkey");
        let collector = &mut *(collector as *mut TablePropertiesCollectorProxy);
        let key = slice::from_raw_parts(key, key_length);
        let value = slice::from_raw_parts(value, value_length);
        collector.collector.add_userkey(key, value, mem::transmute(entry_type))
    }
}

pub extern "C" fn finish(collector: *mut c_void,
                     c_keys: *mut *mut *mut c_char,
                     pair_count  : *mut c_int,
                     c_values: *mut *mut *mut c_char) {
    unsafe {
        let collector = &mut *(collector as *mut TablePropertiesCollectorProxy);
        let props = collector.collector.finish();
        let count = props.len();
        let keys = malloc(size_of::<*mut c_char>() * count) as *mut *mut c_char;
        let values = malloc(size_of::<*mut c_char>() * count) as *mut *mut c_char;

        for (i, (key, value)) in props.iter().enumerate() {
            *keys.offset(i as isize) = malloc(size_of::<c_char>() * (key.len() + 1)) as *mut c_char;
            ptr::copy(key.as_bytes().as_ptr() as *const c_char,
                             *keys.offset(i as isize),
                             key.len());
            *(*keys.offset(i as isize)).offset(key.len() as isize) = 0;

            *values.offset(i as isize) = malloc(size_of::<c_char>() * (value.len() + 1)) as *mut c_char;
            ptr::copy(value.as_bytes().as_ptr() as *const c_char,
                             *values.offset(i as isize),
                             value.len());
            *(*values.offset(i as isize)).offset(value.len() as isize) = 0;
        }
        *c_keys = keys;
        *c_values = values;
        *pair_count = count as c_int;
    }
}

pub extern "C" fn readable_properties(collector: *mut c_void) {
    unsafe {
        Box::from_raw(collector as *mut TablePropertiesCollectorProxy);
    }
}

pub struct TablePropertiesCollectorHandle {
    pub inner: *mut DBTablePropertiesCollector,
}

impl Drop for TablePropertiesCollectorHandle {
    fn drop(&mut self) {
        unsafe {    
            crocksdb_ffi::crocksdb_tablepropertiescollector_destroy(self.inner);
        }
    }
}


pub unsafe fn new_table_properties_collector(c_name: CString,
                                    need_compact: bool,
                                    f: Box<TablePropertiesCollector>)
                                    -> Result<TablePropertiesCollectorHandle, String> {
    let proxy = Box::into_raw(Box::new(TablePropertiesCollectorProxy {
        name: c_name,
        collector: f,
    }));
    let collector = crocksdb_ffi::crocksdb_tablepropertiescollector_create(proxy as *mut c_void,
                                                                destructor,
                                                                add_userkey,
                                                                finish,
                                                                readable_properties,
                                                                name);
    crocksdb_ffi::crocksdb_tablepropertiescollector_set_need_compact(collector, need_compact);
    Ok(TablePropertiesCollectorHandle { inner: collector })
}
