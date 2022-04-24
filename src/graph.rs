use crate::parser::NetexData;

#[derive(Clone, Default, Debug)]
pub struct Node {
    pub short_name: String,
    pub long: f32,
    pub lat: f32,
}

#[derive(Debug)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<(usize, usize)>,
}

#[derive(Clone, Copy)]
struct Indices {
    node: usize,
    data: usize,
    stop: usize,
}

impl Graph {
    pub fn from_data(data: &[NetexData]) -> Graph {
        // short name to scheduled point stop index
        let mut node_map = std::collections::HashMap::<String, Indices>::new();
        let mut ref_to_node_idx = std::collections::HashMap::<String, Indices>::new();
        let mut counter = 0_usize;
        for (data_idx, one_data) in data.iter().enumerate() {
            for (stop_idx, stop) in one_data.scheduled_stop_points.iter().enumerate() {
                if node_map.contains_key(&stop.short_name) {
                    ref_to_node_idx.insert(stop.id.clone(), node_map[&stop.short_name]);
                } else {
                    let indices = Indices {
                        data: data_idx,
                        node: counter,
                        stop: stop_idx,
                    };
                    node_map.insert(stop.short_name.clone(), indices);
                    ref_to_node_idx.insert(stop.id.clone(), indices);
                    counter += 1;
                }
            }
        }
        let mut nodes = vec![Node::default(); node_map.len()];
        for idx in node_map.values() {
            let current = &data[idx.data].scheduled_stop_points[idx.stop];
            nodes[idx.node] = Node {
                short_name: current.short_name.clone(),
                long: current.long,
                lat: current.lat,
            };
        }

        let mut edges = Vec::<(usize, usize)>::new();

        for one_data in data {
            for sequence in &one_data.points_in_squence {
                for (pre, current) in sequence.stops.iter().zip(sequence.stops.iter().skip(1)) {
                    edges.push((
                        ref_to_node_idx[&pre.scheduled_stop_point].node,
                        ref_to_node_idx[&current.scheduled_stop_point].node,
                    ));
                }
            }
        }
        edges.sort_unstable();
        edges.dedup();
        Graph { nodes, edges }
    }
}
