use crate::parser::NetexData;

#[derive(Debug)]
pub struct Node {
    short_name: String,
    pub long: f32,
    pub lat: f32,
}

#[derive(Debug)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<(usize, usize)>,
}

impl Graph {
    pub fn from_data(data: &NetexData) -> Graph {
        // short name to scheduled point stop index
        let mut node_map = std::collections::HashMap::<String, usize>::new();
        let mut ref_to_node_idx = std::collections::HashMap::<String, usize>::new();
        for (idx, stop) in data.scheduled_stop_points.iter().enumerate() {
            if !node_map.contains_key(&stop.short_name) {
                node_map.insert(stop.short_name.clone(), idx);
                ref_to_node_idx.insert(stop.id.clone(), idx);
            } else {
                ref_to_node_idx.insert(stop.id.clone(), node_map[&stop.short_name]);
            }
        }
        let mut nodes = Vec::<Node>::new();
        for idx in node_map.values() {
            let current = &data.scheduled_stop_points[*idx];
            nodes.push(Node {
                short_name: current.short_name.clone(),
                long: current.long,
                lat: current.lat,
            })
        }

        let mut edges = Vec::<(usize, usize)>::new();
        for sequence in &data.points_in_squence {
            for (pre, current) in sequence.stops.iter().zip(sequence.stops.iter().skip(1)) {
                edges.push((
                    ref_to_node_idx[&pre.scheduled_stop_point],
                    ref_to_node_idx[&current.scheduled_stop_point],
                ));
            }
        }
        edges.dedup();
        Graph {
            nodes: nodes,
            edges: edges,
        }
    }
}
