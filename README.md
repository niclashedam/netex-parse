# netex-parse

![Build Status](https://img.shields.io/github/actions/workflow/status/Nuckal777/netex-parse/test.yaml?branch=master)

Multi-threaded parser for public transport data in the [NeTEx](https://netex-cen.eu/) format build with Rust, which generates graphs for stations and connections with timetables as output.
It can produce CSV files or a custom memory-mappable binary format.
During processing non-sensical data is filtered.

## Features
- Multi-threaded parsing of NeTEx documents directly from compressed zipfiles
- Generation of CSV files of contained stations and connections for import into [Neo4j](https://neo4j.com/)
- Generation of a custom binary format with walk duration data between stations
- Filtering of non-sensical data during processing

## Building

```sh
cargo build --release
```

## Usage

All NeTEx documents need to be provided as a zipfile.

### CSV

To generate CSV files, use the following command:
```sh
netex-parse --output-format csv path/to/file.zip
```

The resulting `nodes.csv` and `edges.csv` files can be imported into Neo4j using the provided Cypher statements below:

```cypher
CREATE TEXT INDEX stop_name_index IF NOT EXISTS FOR (n:Stop) ON (n.name)

LOAD CSV FROM "file:///nodes.csv" AS node MERGE (:Stop { name: node[0], loc: point({longitude: toFloat(node[1]), latitude: toFloat(node[2])}) })

LOAD CSV FROM "file:///edges.csv" AS edge MATCH (a:Stop),(b:Stop) WHERE a.name = edge[0] and b.name = edge[1] MERGE (a)-[c:Connection]->(b) ON CREATE SET c.timetable = [edge[2]] ON MATCH SET c.timetable = c.timetable + [edge[2]]
```

### Binary

To generate the binary format, use the following command:

```sh
netex-parse --output-format binary path/to/file.zip
```

The metadata for the resulting `graph.bin` file is written to the `nodes.json` file.
The later contains a mapping from station ids to their names.

### Including walkways with OpenRouteService

Walkway data between stations can optionally be included in the binary output.
Given a properly setup instance of [OpenRouteService](https://openrouteservice.org/) listening on `localhost:8082`, the following procedure includes the walkway data:

1. Generate a `nodes.csv` file using the CSV output option and add the following header: `name,lng,lat,id`.
2. Run the `python ors/walkways.py path/to/nodes.csv` script, which generates a `walk.json` file. If necessary, create a virtual environment.
3. Invoke `netex-parse --walkways path/to/walk.json --output-format binary path/to/netex.zip`
