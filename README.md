# PlanetScale Airbyte Source

## Getting Support

If you run into any issues with using this source, please reach out to PlanetScale Support at `support@planetscale.com`

## About 
This repository builds a docker container that is used a [Source Connector](https://docs.airbyte.com/understanding-airbyte/airbyte-specification#source) in [Airbyte](https://airbyte.com/).

Official documentation of connecting to PlanetScale databases from your Airbyte installation is available [here](https://planetscale.com/docs/integrations/airbyte)

## Self-hosting

Click [here](docs/airbyte.md) for docs to self-host this docker image.

### How the container will be called:
The first argument passed to the image must be the command (e.g. spec, check, discover, read).
Additional arguments can be passed after the command.
Note: The system running the container will handle mounting the appropriate paths so that the config files are available to the container.
```
docker run --rm -i <source-image-name> spec

docker run --rm -i <source-image-name> check --config <config-file-path>

docker run --rm -i <source-image-name> discover --config <config-file-path>

docker run --rm -i <source-image-name> read --config <config-file-path> \
 --catalog <catalog-file-path> [--state <state-file-path>] > message_stream.json
```

```
Interface Pseudocode:
spec() -> ConnectorSpecification
check(Config) -> AirbyteConnectionStatus
discover(Config) -> AirbyteCatalog
read(Config, ConfiguredAirbyteCatalog, State) -> Stream<AirbyteMessage>
```

### Features supported.
1. Connection check for a given set of credentials.
2. Schema fetch for a given database
3. Reading all records for a given table.
4. Incremental data fetch, based on VGTID.
5. Support for sharded databases.
6. Peek before Sync.

## Running the application locally.

### 1. Check command:
Create a json file that has connection variables for the PlanetScale database, which looks like this:


``` json
{
    "host": "<FQDN for your PS database>",
    "database":"<default keyspace name>",
    "username":"<username>",
    "password":"<some password for your database>"
}
```

save it as `source.json` in this directory

Now, you can run `go run main.go check --config source.json` and should see an output like this :


``` bash
 go run main.go check --config source.json | jq .
{
  "type": "CONNECTION_STATUS",
  "connectionStatus": {
    "status": "SUCCEEDED",
    "message": "Successfully connected to database planetscaledatabase at host 7hnhokoiid3c.us-east-3.psdb.cloud with username tzmqspqq1wrz"
  }
}
```

### 2. Discover command:

We need the same file as we do for `read` command above.

You can now run `go run main.go discover --config source.json` and see an output similar t:


``` bash
go run main.go discover --config source.json | jq .
{
  "type": "CATALOG",
  "catalog": {
    "streams": [
      {
        "name": "departments",
        "json_schema": {
          "type": "object",
          "properties": {
            "dept_name": {
              "type": "string"
            },
            "dept_no": {
              "type": "string"
            }
          }
        },
        "supported_sync_modes": [
          "full_refresh"
        ],
        "namespace": "planetscaledatabase"
      }
    ]
  }
}
```

### 3. Read command:
Create a `catalog.json` file that looks similar to the output of "Discover", but with the format of:
```json
{
  "streams": [
    {
      "stream": { // body of stream from above },
      "sync_mode": "full_refresh" // one of the supported_sync_modes from above
    }
  ]
}
```
For the example output above, a valid `catalog.json` might look like:
```json
{
  "streams": [
    {
      "stream": {
        "name": "departments",
        "json_schema": {
          "type": "object",
          "properties": {
            "dept_name": {
              "type": "string"
            },
            "dept_no": {
              "type": "string"
            }
          }
        },
        "supported_sync_modes": [
          "full_refresh"
        ],
        "namespace": "planetscaledatabase"
      },
      "sync_mode": "full_refresh"
    }
  ]
}
```

Then run:
```bash
go run main.go read --config source.json --catalog catalog.json
```

### 3a. Starting from a specific GTID
You can start replication from a specific GTID _per keyspace shard_, by setting `starting_gtids` in your configuration file:
```json
{
    "host": "<FQDN for your PS database>",
    "database":"<default keyspace name>",
    "username":"<username>",
    "password":"<some password for your database>",
    "starting_gtids": "{\"keyspace\": {\"shard\": \"MySQL56/MYGTID:1-3\"}}"
}
```

**Note:** When `starting_gtids` is specified in the configuration file, _and_ a `--state` file is passed, the `--state` file will always take precedence. This is so incremental sync continues working.

**How to get starting GTIDs**

You can get the latest exectued GTID for every shard by querying your database. 
1. Access your PlanetScale database. One way to do so is to use `pscale shell`.
2. Target the keyspace and shard that you would like the latest GTID for by doing `use keyspace/shard`.
    - i.e. `use my_sharded_keyspace/-10`
    - If your database is _unsharded_, you don't have to target a keyspace or shard. Skip this step.
3. Execute `select @@gtid_executed;`
4. You'll get a result that looks something like:
```
my_sharded_keyspace/-10> select @@gtid_executed\G
*************************** 1. row ***************************
@@`gtid_executed`: 16cec08d-f91b-11ee-8afb-aacaf4984ae5:1-5639808,
b13c2fe0-f91a-11ee-aa81-6251c27c9c24:1-2487289,
fe8e2a3c-f91a-11ee-9812-82f5834c1ba7:1-46602355
1 row in set (0.01 sec)
```
5. Use the GTIDs returned to form the starting GTID for that shard (in this example, shard `-10`):
```
{"my_sharded_keyspace": {"-10": "MySQL56/16cec08d-f91b-11ee-8afb-aacaf4984ae5:1-5639808,b13c2fe0-f91a-11ee-aa81-6251c27c9c24:1-2487289,fe8e2a3c-f91a-11ee-9812-82f5834c1ba7:1-46602355"}}
```
6. Repeat this process for all your shards, if your database is sharded.

**Note**: Remember to prepend the prefix `MySQL56/` onto your starting GTIDs.