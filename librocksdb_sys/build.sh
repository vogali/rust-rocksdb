#!/usr/bin/env bash

set -e

con=1
if [[ -f /proc/cpuinfo ]]; then
    con=`grep -c processor /proc/cpuinfo`
else
    con=`sysctl -n hw.ncpu 2>/dev/null || echo 1`
fi

function error() {
    echo $@ >&2
    return 1
}

project_root=`dirname $0`

retry=3

function compile_z() {
    if [[ -f libz.a ]]; then
        return
    fi

    rm -rf zlib
    cp -r $project_root/zlib ./
    cd zlib
    CFLAGS='-fPIC' ./configure --static
    make -j $con
    cp libz.a ../
    cd ..
}

function compile_bz2() {
    if [[ -f libbz2.a ]]; then
        return
    fi

    rm -rf bzip2
    cp -r $project_root/bzip2 ./
    cd bzip2
    make CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64' -j $con
    cp libbz2.a ../
    cd ..
}

function compile_snappy() {
    if [[ -f libsnappy.a ]]; then
        return
    fi

    rm -rf snappy
    cp -r $project_root/snappy ./
    cd snappy
    ./autogen.sh
    ./configure --with-pic --enable-static
    make -j $con
    mv .libs/libsnappy.a ../
    cd ..
}

function compile_lz4() {
    if [[ -f liblz4.a ]]; then
        return
    fi

    rm -rf lz4
    cp -r $project_root/lz4 ./
    cd lz4/lib
    make CFLAGS='-fPIC' all -j $con
    mv liblz4.a ../../
    cd ../..
}

function compile_rocksdb() {
    if [[ -f librocksdb.a ]]; then
        return
    fi

    echo building rocksdb
    rm -rf rocksdb
    cp -r $project_root/rocksdb ./
    wd=`pwd`
    cd rocksdb
    cp $CROCKSDB_PATH/c.cc ./db/c.cc
    cp $CROCKSDB_PATH/rocksdb/c.h ./include/rocksdb/c.h
    export EXTRA_CFLAGS="-fPIC -I${wd}/zlib -I${wd}/bzip2 -I${wd}/snappy -I${wd}/lz4/lib"
    export EXTRA_CXXFLAGS="-DZLIB -DBZIP2 -DSNAPPY -DLZ4 $EXTRA_CFLAGS"
    make static_lib -j $con
    mv librocksdb.a ../
    cd ..
}

function find_library() {
    if [[ "$CXX" = "" ]]; then
        if g++ --version &>/dev/null; then
            CXX=g++
        elif clang++ --version &>/dev/null; then
            CXX=clang++
        else
            error failed to find valid cxx compiler.
        fi
    fi

    $CXX --print-file-name $1
}

if [[ $# -eq 0 ]]; then
    error $0 [compile_bz2\|compile_z\|compile_lz4\|compile_rocksdb\|compile_snappy\|find_library]
fi

$@
