use std::time;

use neo4rs::query;
use tokio_stream::StreamExt;

pub struct ConnectionParameters {
    pub uri: String,
    pub user: String,
    pub password: String,
}

pub fn push_graph_sync(
    graph: &crate::graph::Graph,
    params: ConnectionParameters,
) -> Result<(), std::io::Error> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async { push_graph(graph, params).await.unwrap() });
    Ok(())
}

async fn push_graph(
    graph: &crate::graph::Graph,
    params: ConnectionParameters,
) -> Result<(), neo4rs::Error> {
    let config = neo4rs::config()
        .uri(&params.uri)
        .user(&params.user)
        .password(&params.password)
        .db("neo4j")
        .build()?;
    let client = neo4rs::Graph::connect(config).await?;
    println!("creating index on stop names...");
    let index = query("CREATE TEXT INDEX stop_name_index IF NOT EXISTS FOR (n:Stop) ON (n.name)");
    client.run(index).await?;
    
    let mut node_stream = tokio_stream::iter(graph.nodes.chunks(255));
    println!("pushing nodes...");
    let start = time::Instant::now();
    while let Some(node_chunk) = node_stream.next().await {
        let mut objects = node_chunk
            .iter()
            .fold("".to_owned(), |mut acc, node| {
                acc.push_str(&format!(
                    "{{name: \"{}\", lat: {}, long: {} }},",
                    node.short_name, node.lat, node.long
                ));
                acc
            });
        objects.pop();
        let query_str = format!("UNWIND [{}] AS node MERGE (:Stop {{ name: node.name, loc: point({{longitude: node.long, latitude: node.lat}}) }})", objects);
        let create = query(&query_str);
        client.run(create).await?;
    }
    let end = time::Instant::now();
    println!("pushing nodes took: {}s", (end - start).as_secs());

    println!("pushing edges...");
    let mut edge_stream = tokio_stream::iter(graph.edges.chunks(255));
    while let Some(edge_chunk) = edge_stream.next().await {
        let mut objects = edge_chunk.iter().fold("".to_owned(), |mut acc, e| {
            let journeys = serde_json::to_string(&e.timetable).expect("failed to serialize json");
            acc.push_str(&format!(
                "{{ start: \"{}\", end: \"{}\", timetable: '{}' }},",
                graph.nodes[e.start_node].short_name,
                graph.nodes[e.end_node].short_name,
                journeys,
            ));
            acc
        });
        objects.pop();
        let query_str = format!("UNWIND [{}] AS edge MATCH (a:Stop),(b:Stop) WHERE a.name = edge.start and b.name = edge.end MERGE (a)-[c:Connection]->(b) ON CREATE SET c.timetable = [edge.timetable] ON MATCH SET c.timetable = c.timetable + [edge.timetable]", objects);
        // {{journeys: edge.journeys}}
        // let create = query("UNWIND $objects AS edge MATCH (a:Stop),(b:Stop) WHERE a.name = edge.start and b.name = edge.end CREATE (a)-[:Connection {departure: edge.departure, arrival: edge.arrival}]->(b)").param("objects", objects);
        let create = query(&query_str);
        client.run(create).await?;
        println!("pushed some edges :)");
    }
    Ok(())
}
