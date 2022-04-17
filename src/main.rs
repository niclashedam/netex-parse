use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use zip::ZipArchive;

use crate::parser::NetexData;

mod graph;
mod parser;

fn main() {
    let zip_stream = std::fs::File::open("20220328_fahrplaene_gesamtdeutschland.zip")
        .expect("failed to open data");
    let zip_memmap = unsafe { memmap::Mmap::map(&zip_stream).expect("failed mmap") };
    let zip_cursor = std::io::Cursor::new(&zip_memmap);
    let archive = ZipArchive::new(zip_cursor).expect("failed to read zip");
    let documents: Vec<String> = archive
        .file_names()
        .filter(|name| name.contains(".xml"))
        .map(|name| name.to_owned())
        .collect();
    parse(&zip_memmap, "DBDB", &documents)
}

fn parse(archive: &memmap::Mmap, key: &str, documents: &[String]) {
    let data = documents
        .par_iter()
        .progress_count(documents.len() as u64)
        .map(|doc| {
            let zip_cursor = std::io::Cursor::new(archive);
            let mut archive = ZipArchive::new(zip_cursor).expect("failed to read zip");
            let file = archive.by_name(&doc).expect("failed to find document");
            if file.is_dir() {
                return NetexData::default();
            }
            let size = file.size() as usize;
            parser::NetexData::from_xml(file, size).unwrap_or_default()
        })
        .reduce(
            || NetexData::default(),
            |mut accum, item| {
                accum.append(item);
                accum
            },
        );
    println!("deduping...");
    let graph = graph::Graph::from_data(&data);
    println!(
        "{} has {} deduped nodes and {} deduped edges and {} timetabled routes.",
        key,
        graph.nodes.len(),
        graph.edges.len(),
        data.service_journeys.len()
    );
}
