# Airport Extension for DuckDB

This extension `airport` enables the use of [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) with [DuckDB](https://duckdb.org).

![Ducks waiting to take a flight at the airport](./duckdb-airport-1.jpg)

## What is Arrow Flight?

<img src="https://arrow.apache.org/docs/_static/arrow.png" style="float:right" width="200px" alt="Apache Arrow Logo"/>
Arrow Flight is an RPC framework for high-performance data services based on [Apache Arrow](https://arrow.apache.org/docs/index.html), and is built on top of [gRPC](https://grpc.io) and the [Arrow IPC format](https://arrow.apache.org/docs/format/IPC.html).

## API

### Listing Flights

```airport_list_flights(VARCHAR)```

This function returns a list of Arrow Flights that are available at a particular endpoint.

```sql
select * from airport_list_flights('http://127.0.0.1:8815');

flight_descriptor = [uploaded.parquet]
         endpoint = [{'ticket': uploaded.parquet, 'location': [grpc://0.0.0.0:8815], 'expiration_time': NULL, 'app_metadata': }]
          ordered = false
    total_records = 3
      total_bytes = 363
     app_metadata =
           schema = Character: string
```






## Getting started

First step to getting started is to create your own repo from this template by clicking `Use this template`. Then clone your new repository using
```sh
git clone --recurse-submodules git@github.com:rustyconover/duckdb-arrow-flight-extension.git
```
Note that `--recurse-submodules` will ensure DuckDB is pulled which is required to build the extension.

## Building
### Managing dependencies

The airport extension utilizes VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/airport/airport.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `airport.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`. This shell will have the extension pre-loaded.

Now we can use the features from the extension directly in DuckDB.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

