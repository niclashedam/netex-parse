# netex-parse
Multi-threaded netex parser that generates csv files containing nodes and edges that can be imported into neo4j using the statements in `cypher.txt`.
Netex data has to be provided as a zip file.

## Usage
```
cargo run --release
```
The file name can be changed in `main.rs`
