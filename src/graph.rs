use crate::parser::{Authority, Line, NetexData, UicOperatingPeriod};

#[derive(Clone, Default, Debug)]
pub struct Node {
    pub short_name: String,
    pub long: f32,
    pub lat: f32,
}

#[derive(Debug, serde::Serialize)]
pub struct Journey {
    #[serde(rename(serialize = "d"))]
    pub departure: u16,
    #[serde(rename(serialize = "a"))]
    pub arrival: u16,
    #[serde(rename(serialize = "t"))]
    pub transport_mode: String,
    #[serde(rename(serialize = "o"))]
    pub operating_period: usize,
    #[serde(rename(serialize = "l"))]
    pub line: String,
    #[serde(rename(serialize = "c"))]
    pub controller: String,
}

#[derive(Debug, serde::Serialize)]
pub struct OperatingPeriod {
    #[serde(rename(serialize = "f"))]
    pub from: u32,
    #[serde(rename(serialize = "t"))]
    pub to: u32,
    #[serde(rename(serialize = "v"))]
    pub valid_day_bits: String,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct Timetable {
    #[serde(rename(serialize = "j"))]
    pub journeys: Vec<Journey>,
    #[serde(rename(serialize = "p"))]
    pub periods: Vec<OperatingPeriod>,
}

#[derive(Debug)]
pub struct Edge {
    pub start_node: usize,
    pub end_node: usize,
    pub timetable: Timetable,
}

#[derive(Debug)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
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

        let mut point_in_journey_to_stop_ref = std::collections::HashMap::<String, String>::new();
        for one_data in data {
            for sequence in &one_data.service_journey_patterns {
                for stop in &sequence.stops {
                    point_in_journey_to_stop_ref
                        .entry(stop.id.clone())
                        .or_insert(stop.scheduled_stop_point.clone());
                }
            }
        }

        let mut lines = std::collections::HashMap::<String, Line>::new();
        for one_data in data {
            for line in &one_data.lines {
                lines.insert(line.id.clone(), line.clone());
            }
        }

        let mut authorities = std::collections::HashMap::<String, Authority>::new();
        for one_data in data {
            for authority in &one_data.authorities {
                authorities.insert(authority.id.clone(), authority.clone());
            }
        }

        let mut pattern_ref_to_line = std::collections::HashMap::<String, String>::new();
        for one_data in data {
            for journey_pattern in &one_data.service_journey_patterns {
                pattern_ref_to_line.insert(journey_pattern.id.clone(), journey_pattern.line.clone());
            }
        }

        let mut edges = std::collections::HashMap::<(usize, usize), Edge>::new();
        let mut periods =
            std::collections::HashMap::<(usize, usize), Vec<UicOperatingPeriod>>::new();
        for one_data in data {
            for journey in &one_data.service_journeys {
                for window in journey.passing_times.windows(2) {
                    let pre = &window[0];
                    let current = &window[1];
                    let start_node = ref_to_node_idx
                        [&point_in_journey_to_stop_ref[&pre.stop_point_in_journey_pattern]]
                        .node;
                    let end_node = ref_to_node_idx
                        [&point_in_journey_to_stop_ref[&current.stop_point_in_journey_pattern]]
                        .node;
                    let period = one_data
                        .day_type_assignments
                        .iter()
                        .find(|da| da.day_type == journey.day_type)
                        .expect("Day type without operating period found")
                        .operating_period
                        .clone();
                    let period_entry = periods.entry((start_node, end_node)).or_default();
                    let mut period_idx = period_entry
                        .iter()
                        .enumerate()
                        .find(|(_, p)| p.id == period)
                        .map(|(idx, _)| idx);
                    if period_idx.is_none() {
                        period_idx = Some(period_entry.len());
                        let op = one_data
                            .operating_periods
                            .iter()
                            .find(|p| p.id == period)
                            .expect("undefined operating period");
                        period_entry.push(op.clone());
                    }

                    let entry = edges.entry((start_node, end_node)).or_insert(Edge {
                        start_node: start_node,
                        end_node: end_node,
                        timetable: Timetable::default(),
                    });
                    let line = &lines[&pattern_ref_to_line[&journey.pattern_ref]];
                    entry.timetable.journeys.push(Journey {
                        departure: pre.departure,
                        arrival: current.arrival,
                        transport_mode: journey.transport_mode.clone(),
                        operating_period: period_idx.unwrap(),
                        line: line.short_name.clone(),
                        controller: authorities[&line.authority].short_name.clone(),
                    });
                }
            }
        }

        for (nodes, ops) in periods.into_iter() {
            edges
                .get_mut(&nodes)
                .expect("unknown edge")
                .timetable
                .periods = ops
                .into_iter()
                .map(|op| OperatingPeriod {
                    from: op.from,
                    to: op.to,
                    valid_day_bits: base64::encode(op.valid_day_bits),
                })
                .collect();
        }
        Graph {
            nodes,
            edges: edges.into_iter().map(|(_, e)| e).collect(),
        }
    }
}
