use std::io::Write;

use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use zip::ZipArchive;

use crate::{graph::WalkEdge, parser::NetexData};

mod graph;
mod parser;

fn main() {
    let zip_stream = std::fs::File::open("20230925_fahrplaene_gesamtdeutschland.zip")
        .expect("failed to open data");
    let zip_memmap = unsafe { memmap::Mmap::map(&zip_stream).expect("failed mmap") };
    let zip_cursor = std::io::Cursor::new(&zip_memmap);
    let archive = ZipArchive::new(zip_cursor).expect("failed to read zip");
    let documents: Vec<String> = archive.file_names().map(str::to_owned).collect();
    parse(&zip_memmap, "DBDB_80", &documents);
}

fn parse(archive: &memmap::Mmap, key: &str, documents: &[String]) {
    println!("loading walk data");
    let walk_bytes = std::fs::read("walk.json").expect("failed to read walk data");
    let walks: Vec<WalkEdge> =
        serde_json::from_slice(&walk_bytes).expect("failed to deserialize json");
    let mut data = documents
        .par_iter()
        .progress_count(documents.len() as u64)
        // .filter(|doc| doc.contains(key))
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
    println!("deduping...");
    for d in &mut data {
        d.scheduled_stop_points.retain(|stop| {
            stop.long > 5.5 && stop.long < 15.5 && stop.lat > 47.0 && stop.lat < 55.5
        });
    }
    let graph = graph::Graph::from_data(&data, &walks);
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
    // dump_csv(&graph).expect("failed to dump csv");
    dump_binary(&graph).expect("failed to dump binary");
}

fn dump_csv(graph: &graph::Graph) -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true);
    let mut node_writer = std::io::BufWriter::new(opts.open("./nodes.csv")?);
    for node in &graph.nodes {
        node_writer.write_all(
            format!(
                "\"{}\",{},{},{}\n",
                node.short_name, node.long, node.lat, node.id
            )
            .as_bytes(),
        )?;
    }
    node_writer.flush()?;

    let mut edge_writer = std::io::BufWriter::new(opts.open("./edges.csv")?);
    for edge in &graph.edges {
        let timetable = serde_json::to_string(&edge.timetable)
            .expect("failed to serialize json")
            .replace('"', "\\\"");
        edge_writer.write_all(
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

#[derive(serde::Serialize)]
struct MetaNode {
    name: String,
    // large u64 do not survive JSON.parse over in JSLand
    // so we use a string here
    id: String,
    coords: [f32; 2]
}

fn dump_binary(graph: &graph::Graph) -> Result<(), Box<dyn std::error::Error>> {
    fn node_as_bytes(node: &graph::Node) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // id is implicit
        let mut data = Vec::<u8>::new();
        let mut writer = std::io::Cursor::new(&mut data);
        writer.write_all(&node.id.to_le_bytes())?;
        writer.write_all(&node.lat.to_le_bytes())?;
        writer.write_all(&node.long.to_le_bytes())?;
        let name_bytes = node.short_name.as_bytes();
        writer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(name_bytes)?;
        Ok(data)
    }

    fn period_as_bytes(
        period: &graph::OperatingPeriod,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut data = Vec::<u8>::new();
        let mut writer = std::io::Cursor::new(&mut data);
        writer.write_all(&period.from.to_le_bytes())?;
        writer.write_all(&period.to.to_le_bytes())?;
        writer.write_all(&(period.valid_day.len() as u32).to_le_bytes())?;
        writer.write_all(&period.valid_day)?;
        Ok(data)
    }

    fn edge_as_bytes(edge: &graph::Edge) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut data = Vec::<u8>::new();
        let mut writer = std::io::Cursor::new(&mut data);
        writer.write_all(&(edge.start_node as u32).to_le_bytes())?;
        writer.write_all(&(edge.end_node as u32).to_le_bytes())?;
        writer.write_all(&edge.walk_seconds.to_le_bytes())?;
        let journeys = &edge.timetable.journeys;
        // arrival, departure, operating period -> 3x u16
        writer.write_all(&((journeys.len() * 6) as u32).to_le_bytes())?;
        for journey in journeys {
            writer.write_all(&journey.arrival.to_le_bytes())?;
            writer.write_all(&journey.departure.to_le_bytes())?;
            writer.write_all(&(journey.operating_period as u16).to_le_bytes())?;
        }
        let mut periods = Vec::<u8>::new();
        for period in &edge.timetable.periods {
            periods.extend(period_as_bytes(period)?);
        }
        writer.write_all(&(periods.len() as u32).to_le_bytes())?;
        writer.write_all(&periods)?;
        Ok(data)
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    let mut writer = std::io::BufWriter::new(opts.open("./graph.bin")?);
    // TODO: magic number, file version
    // nodes with data
    let mut node_data = Vec::<u8>::new();
    let mut node_writer = std::io::Cursor::new(&mut node_data);
    for node in &graph.nodes {
        node_writer.write_all(&node_as_bytes(node)?)?;
    }
    writer.write_all(&(graph.nodes.len() as u32).to_le_bytes())?;
    writer.write_all(&node_data)?;
    // edges with data
    let mut edge_data = Vec::<u8>::new();
    let mut edge_writer = std::io::Cursor::new(&mut edge_data);
    for edge in &graph.edges {
        edge_writer.write_all(&edge_as_bytes(edge)?)?;
    }
    writer.write_all(&(graph.edges.len() as u32).to_le_bytes())?;
    writer.write_all(&edge_data)?;
    writer.flush()?;

    let metas: Vec<MetaNode> = graph.nodes.iter().map(|n| MetaNode {
        coords: [n.long, n.lat],
        id: n.id.to_string(),
        name: n.short_name.clone(),
    }).collect();
    std::fs::write("nodes.json", &serde_json::to_vec(&metas)?)?;
    Ok(())
}
