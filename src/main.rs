use std::io::Write;

use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use zip::ZipArchive;

use crate::parser::NetexData;

mod graph;
mod neo4j;
mod parser;

fn main() {
    let zip_stream = std::fs::File::open("20220328_fahrplaene_gesamtdeutschland.zip")
        .expect("failed to open data");
    let zip_memmap = unsafe { memmap::Mmap::map(&zip_stream).expect("failed mmap") };
    let zip_cursor = std::io::Cursor::new(&zip_memmap);
    let archive = ZipArchive::new(zip_cursor).expect("failed to read zip");
    let documents: Vec<String> = archive
        .file_names()
        .filter(|name| name.contains("DBDB"))
        .map(str::to_owned)
        .collect();
    parse(&zip_memmap, "DBDB", &documents);
}

fn parse(archive: &memmap::Mmap, key: &str, documents: &[String]) {
    let data = documents
        .par_iter()
        .progress_count(documents.len() as u64)
        .map(|doc| {
            let zip_cursor = std::io::Cursor::new(archive);
            let mut archive = ZipArchive::new(zip_cursor).expect("failed to read zip");
            let file = archive.by_name(doc).expect("failed to find document");
            if file.is_dir() {
                return Vec::new();
            }
            let size = file.size().try_into().expect("u64 does not fit usize");
            vec![parser::NetexData::from_xml(file, size).unwrap_or_default()]
        })
        .reduce(Vec::<NetexData>::new, |mut accum, item| {
            accum.extend(item);
            accum
        });
    drop(archive);
    println!("deduping...");
    let graph = graph::Graph::from_data(&data);
    let route_count: usize = data.iter().map(|d| d.service_journeys.len()).sum();
    let line_count: usize = data.iter().map(|d| d.lines.len()).sum();
    println!(
        "{} has {} deduped nodes and {} deduped edges and {} timetabled routes and {} lines.",
        key,
        graph.nodes.len(),
        graph.edges.len(),
        route_count,
        line_count,
    );
    drop(data);
    // neo4j::push_graph_sync(
    //     &graph,
    //     neo4j::ConnectionParameters {
    //         uri: "localhost:7687".to_owned(),
    //         user: "".to_owned(),
    //         password: "".to_owned(),
    //     },
    // ).unwrap();
    dump_csv(&graph).expect("failed to dump csv");
}

fn dump_csv(graph: &graph::Graph) -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true);
    let mut node_writer = std::io::BufWriter::new(opts.open("./nodes.csv")?);
    for node in &graph.nodes {
        node_writer
            .write(format!("\"{}\",{},{}\n", node.short_name, node.long, node.lat).as_bytes())?;
    }
    node_writer.flush()?;

    let mut edge_writer = std::io::BufWriter::new(opts.open("./edges.csv")?);
    for edge in &graph.edges {
        let timetable = serde_json::to_string(&edge.timetable)
            .expect("failed to serialize json")
            .replace('"', "\\\"");
        edge_writer.write(
            format!(
                "\"{}\",\"{}\",\"{}\"\n",
                graph.nodes[edge.start_node].short_name,
                graph.nodes[edge.end_node].short_name,
                timetable
            )
            .as_bytes(),
        )?;
    }
    edge_writer.flush()?;
    Ok(())
}
