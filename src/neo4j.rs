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
    let mut node_stream = tokio_stream::iter(graph.nodes.chunks(256));
    println!("pushing nodes...");
    while let Some(node_chunk) = node_stream.next().await {
        let mut query_str = node_chunk
            .iter()
            .fold("CREATE ".to_owned(), |mut acc, node| {
                acc.push_str(&format!(
                    "(:Stop {{name: \"{}\", lat: {}, long: {} }}),",
                    node.short_name, node.lat, node.long
                ));
                acc
            });
        query_str.pop();
        let create = query(&query_str);
        client.run(create).await?;
    }

    println!("pushing edges...");
    let mut edge_stream = tokio_stream::iter(graph.edges.chunks(256));
    while let Some(edge_chunk) = edge_stream.next().await {
        let queries: Vec<neo4rs::Query> = edge_chunk.iter().map(|(id0, id1)| {
            let name0 = &graph.nodes[*id0].short_name;
            let name1 = &graph.nodes[*id1].short_name;
            let query_str = format!("MATCH (a:Stop),(b:Stop) WHERE a.name = \"{}\" and b.name = \"{}\" CREATE (a)-[:Connection]->(b)", name0, name1);
            query(&query_str)
        }).collect();
        let transaction = client.start_txn().await?;
        transaction.run_queries(queries).await?;
        transaction.commit().await?;
    }
    Ok(())
}
