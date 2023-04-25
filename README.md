# PlanetScale Airbyte Source

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
