# BUILDING

This project: **a-table-store-library**
Version: **0.1.0**

## Local build

```bash
# one-shot build + install
./build.sh install
```

Or run the steps manually:

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j"$(nproc || sysctl -n hw.ncpu || echo 4)"
sudo cmake --install .
```



## Install dependencies (from `deps.libraries`)


### System packages (required)

```bash
sudo apt-get update && sudo apt-get install -y zlib1g-dev
```



### Development tooling (optional)

```bash
sudo apt-get update && sudo apt-get install -y autoconf automake gdb libtool perl python3 python3-pip python3-venv valgrind
```



### the-macro-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/the-macro-library.git" "the-macro-library"
cd "the-macro-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "the-macro-library"
```


### a-memory-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/a-memory-library.git" "a-memory-library"
cd "a-memory-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "a-memory-library"
```


### a-json-sax-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/a-json-sax-library.git" "a-json-sax-library"
cd "a-json-sax-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "a-json-sax-library"
```


### a-json-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/a-json-library.git" "a-json-library"
cd "a-json-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "a-json-library"
```


### the-lz4-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/the-lz4-library.git" "the-lz4-library"
cd "the-lz4-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "the-lz4-library"
```


### a-bloom-filter-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/a-bloom-filter-library.git" "a-bloom-filter-library"
cd "a-bloom-filter-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "a-bloom-filter-library"
```


### ZLIB

Install via package manager:

```bash
sudo apt-get update && sudo apt-get install -y zlib1g-dev
```


### the-io-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/the-io-library.git" "the-io-library"
cd "the-io-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "the-io-library"
```


### sql-parser-library

Clone & build:

```bash
git clone --depth 1 --single-branch "https://github.com/contactandyc/sql-parser-library.git" "sql-parser-library"
cd "sql-parser-library"
./build.sh clean
./build.sh install
cd ..
rm -rf "sql-parser-library"
```

