# Airport Extension for DuckDB

This extension `airport` enables the use of [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) with [DuckDB](https://duckdb.org).

![Ducks waiting to take a flight at the airport](./duckdb-airport-1.jpg)

## What is Arrow Flight?

<img src="https://arrow.apache.org/docs/_static/arrow.png" style="float:right" width="200px" alt="Apache Arrow Logo"/>
Arrow Flight is an RPC framework for high-performance data services based on [Apache Arrow](https://arrow.apache.org/docs/index.html), and is built on top of [gRPC](https://grpc.io) and the [Arrow IPC format](https://arrow.apache.org/docs/format/IPC.html).

## API

### Listing Flights

```airport_list_flights(location, criteria, auth_token="token_value")```

Parameters:

| Parameter Name | Type | Description |
|----------------|------|-------------|
| `location` | VARCHAR | This is the location of the Flight server |
| `criteria` | VARCHAR | This is free form criteria to pass to the Flight server |

Named Parameters:

`auth_token` - a VARCHAR that is used as a bearer token to present to the server, the header is formatted like `Authorization: Bearer <auth_token>`

This function returns a list of Arrow Flights that are available at a particular endpoint.

```sql
> select * from airport_list_flights('http://127.0.0.1:8815', null);

flight_descriptor = [uploaded.parquet]
         endpoint = [{'ticket': uploaded.parquet, 'location': [grpc://0.0.0.0:8815], 'expiration_time': NULL, 'app_metadata': }]
          ordered = false
    total_records = 3
      total_bytes = 363
     app_metadata =
           schema = Character: string
```

In addition to the `criteria` parameter, the Airport extension will pass a GRPC header
that contains a JSON serialized representation of all of the filters that are applied to
the results.

To illustrate this through an example:

```sql
select * from airport_list_flights('grpc://localhost:8815/', null) where total_bytes = 5;
```

The GRPC header `airport-duckdb-json-filters` will be set to

```json
{
 "filters": [
  {
   "expression_class": "BOUND_COMPARISON",
   "type": "COMPARE_EQUAL",
   "left": {
    "expression_class": "BOUND_COLUMN_REF",
    "type": "BOUND_COLUMN_REF",
    "alias": "total_bytes",
    "return_type": {
     "id": "BIGINT"
    }
   },
   "right": {
    "expression_class": "BOUND_CONSTANT",
    "type": "VALUE_CONSTANT",
    "value": {
     "type": {
      "id": "BIGINT"
     },
     "is_null": false,
     "value": 5
    }
   }
  }
 ]
}
```

The GRPC header will not contain newlines, but the JSON has been reformatted for ease of comprehension.

It is up to the implementer of the GRPC server to use this header to apply optimizations.  The Airport DuckDB extension will still apply the filters to the result returned by the server. This means that the filter logic is purely advisory.  If Arrow Flight servers implement the filtering logic server side it can unlock some impressive optimizations.  The JSON schema of the serialized filter expressions is not guaranteed to remain unchanged across DuckDB versions.

### Taking a Flight

```airport_take_flight(location, descriptor, auth_token="token_value")```

Parameters:

| Parameter Name | Type | Description |
|----------------|------|-------------|
| `location` | VARCHAR | This is the location of the Flight server |
| `descriptor` | ANY | This is the descriptor of the flight.  If its a VARCHAR or BLOB its interpreted as a command, if its an ARRAY or LIST of VARCHAR its considered a path based descriptor.  |

Named Parameters:

`auth_token` - a VARCHAR that is used as a bearer token to present to the server, the header is formatted like `Authorization: Bearer <auth_token>`


```sql
select * from airport_take_flight('grpc://localhost:8815/', ['counter-stream']) limit 5;
┌─────────┐
│ counter │
│  int64  │
├─────────┤
│       0 │
│       1 │
│       2 │
│       3 │
│       4 │
└─────────┘
```

## Implementation Notes

### TODO

1. Authorization Support
2. Integration with the DuckDB catalog.
3. Ingestigate the multithreaded endpoint support.
4. Write support?  How to do updates of rows against tables?

### Memory Alignment of Apache Arrow Buffers

This extension applied a change to the C interface of Apache Arrow that enforces 8 byte alignment of the column data.  DuckDB requires that this alignment is present, where Apache Arrow and Arrow Flight do not require this.

If other extensions use Apache Arrow please ensure that the patch `align-record-batch.patch` is applied from `vcpkg-overlay/arrow`.

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

