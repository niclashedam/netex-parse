use std::collections::HashMap;

use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::parser::{Authority, DayTypeAssignment, Line, NetexData, UicOperatingPeriod};

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

#[derive(Clone, Default, Debug, serde::Serialize)]
pub struct OperatingPeriod {
    #[serde(rename(serialize = "f"))]
    pub from: u32,
    #[serde(rename(serialize = "t"))]
    pub to: u32,
    #[serde(rename(serialize = "v"))]
    pub valid_day_bits: String,
    pub valid_day: Vec<u8>,
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
    pub walk_seconds: u16,
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

#[derive(Default, serde::Deserialize)]
pub struct WalkEdge {
    pub start: String,
    pub end: String,
    pub duration: f32,
}

impl Graph {
    #[allow(clippy::too_many_lines)]
    pub fn from_data(data: &[NetexData], walk_seconds: Vec<WalkEdge>) -> Graph {
        // short name to scheduled point stop index
        let mut node_map = HashMap::<String, Indices>::new();
        let mut ref_to_node_idx = HashMap::<u64, Indices>::new();
        let mut counter = 0_usize;
        for (data_idx, one_data) in data.iter().enumerate() {
            for (stop_idx, stop) in one_data.scheduled_stop_points.iter().enumerate() {
                if node_map.contains_key(&stop.short_name) {
                    ref_to_node_idx.insert(stop.id, node_map[&stop.short_name]);
                } else {
                    let indices = Indices {
                        data: data_idx,
                        node: counter,
                        stop: stop_idx,
                    };
                    node_map.insert(stop.short_name.clone(), indices);
                    ref_to_node_idx.insert(stop.id, indices);
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
        // nodes contains stops deduplicated by short name
        // ref_to_node_idx maps a nextex stop ref to a index into nodes

        let mut point_in_journey_to_stop_ref = HashMap::<u64, u64>::new();
        for one_data in data {
            for sequence in &one_data.service_journey_patterns {
                for stop in &sequence.stops {
                    point_in_journey_to_stop_ref
                        .entry(stop.id)
                        .or_insert(stop.scheduled_stop_point);
                }
            }
        }

        let mut lines = HashMap::<u64, Line>::new();
        for one_data in data {
            for line in &one_data.lines {
                lines.insert(line.id, line.clone());
            }
        }

        let mut authorities = HashMap::<u64, Authority>::new();
        for one_data in data {
            for authority in &one_data.authorities {
                authorities.insert(authority.id, authority.clone());
            }
        }

        let mut pattern_ref_to_line = HashMap::<u64, u64>::new();
        for one_data in data {
            for journey_pattern in &one_data.service_journey_patterns {
                pattern_ref_to_line.insert(journey_pattern.id, journey_pattern.line);
            }
        }

        let mut period_map = HashMap::<u64, usize>::new();
        for (idx, period) in data
            .iter()
            .flat_map(|d| d.operating_periods.iter())
            .enumerate()
        {
            period_map.insert(period.id, idx);
        }
        let mut day_type_assignments = HashMap::<u64, DayTypeAssignment>::new();
        for dta in data.iter().flat_map(|d| d.day_type_assignments.iter()) {
            day_type_assignments.insert(dta.day_type, dta.clone());
        }

        let mut edges = data
            .par_iter()
            .progress()
            .flat_map(|d| d.service_journeys.par_iter())
            .map(|journey| {
                let mut local_edges = HashMap::<(usize, usize), Edge>::new();
                for window in journey.passing_times.windows(2) {
                    let pre = &window[0];
                    let current = &window[1];
                    let Some(start_indecies) = ref_to_node_idx
                        .get(&point_in_journey_to_stop_ref[&pre.stop_point_in_journey_pattern])
                    else {
                        continue;
                    };
                    let Some(end_indecies) = ref_to_node_idx
                        .get(&point_in_journey_to_stop_ref[&current.stop_point_in_journey_pattern])
                    else {
                        continue;
                    };
                    let period = day_type_assignments
                        .get(&journey.day_type)
                        .expect("Day type without operating period found")
                        .operating_period;

                    let entry = local_edges
                        .entry((start_indecies.node, end_indecies.node))
                        .or_insert(Edge {
                            walk_seconds: u16::MAX,
                            start_node: start_indecies.node,
                            end_node: end_indecies.node,
                            timetable: Timetable::default(),
                        });
                    let line = &lines[&pattern_ref_to_line[&journey.pattern_ref]];
                    entry.timetable.journeys.push(Journey {
                        departure: pre.departure,
                        arrival: current.arrival,
                        transport_mode: journey.transport_mode.clone(),
                        operating_period: *period_map.get(&period).unwrap(),
                        line: line.short_name.clone(),
                        controller: authorities[&line.authority].short_name.clone(),
                    });
                }
                local_edges
            })
            .reduce(HashMap::<(usize, usize), Edge>::new, |a, mut b| {
                for (key, value) in a {
                    let entry = b.entry(key).or_insert(Edge {
                        walk_seconds: u16::MAX,
                        start_node: key.0,
                        end_node: key.1,
                        timetable: Timetable::default(),
                    });
                    entry
                        .timetable
                        .journeys
                        .extend(value.timetable.journeys.into_iter());
                }
                b
            });

        // loop through walk seconds hashmap
        for walk_edge in walk_seconds {
            let start_idx = node_map[&walk_edge.start].node;
            let end_idx = node_map[&walk_edge.end].node;
            let start_node = &nodes[start_idx];
            let end_node = &nodes[end_idx];
            let distance = great_circle_distance(
                (start_node.long, start_node.lat),
                (end_node.long, end_node.lat),
            );
            if distance > 1.0 {
                continue;
            }
            let mut forward = edges.entry((start_idx, end_idx)).or_insert(Edge {
                start_node: start_idx,
                end_node: end_idx,
                timetable: Timetable::default(),
                walk_seconds: u16::MAX,
            });
            forward.walk_seconds = walk_edge.duration as u16;
            let mut backward = edges.entry((end_idx, start_idx)).or_insert(Edge {
                start_node: end_idx,
                end_node: end_idx,
                timetable: Timetable::default(),
                walk_seconds: u16::MAX,
            });
            backward.walk_seconds = walk_edge.duration as u16;
        }

        edges.par_iter_mut().for_each(|(_, edge)| {
            let mut global_to_local = HashMap::<usize, usize>::new();
            let mut counter = 0;
            for journey in &edge.timetable.journeys {
                if global_to_local.contains_key(&journey.operating_period) {
                    continue;
                }
                global_to_local.insert(journey.operating_period, counter);
                counter += 1;
            }
            let mut local_ops = vec![OperatingPeriod::default(); global_to_local.len()];
            for (global, local) in &global_to_local {
                let uic_op = Self::lookup_operating_period(data, *global).expect(
                    "failed to map global operating period index to concrete operating period",
                );
                local_ops[*local] = OperatingPeriod {
                    from: uic_op.from,
                    to: uic_op.to,
                    valid_day_bits: base64::encode(&uic_op.valid_day_bits),
                    valid_day: uic_op.valid_day_bits.clone(),
                }
            }
            for journey in &mut edge.timetable.journeys {
                journey.operating_period = *global_to_local
                    .get(&journey.operating_period)
                    .expect("failed to map global to local operating period");
            }
            edge.timetable.periods = local_ops;
        });

        // filter non-sensical journeys
        edges.par_iter_mut().for_each(|(_, edge)| {
            let start_node = &nodes[edge.start_node];
            let end_node = &nodes[edge.end_node];
            let distance = great_circle_distance(
                (start_node.long, start_node.lat),
                (end_node.long, end_node.lat),
            );
            edge.timetable.journeys.retain(|j| {
                let departure_min = (j.departure % 60) + ((j.departure / 60) * 60);
                let mut arrival_min = (j.arrival % 60) + ((j.arrival / 60) * 60);
                if arrival_min < departure_min {
                    arrival_min += 24 * 60;
                }
                let minutes = arrival_min - departure_min;
                let hours = minutes as f32 / 60.0;
                let speed = distance / hours;
                speed < 325.0 || (minutes < 3 && distance < 3.0)
            });
        });

        Graph {
            nodes,
            edges: edges.into_iter().map(|(_, e)| e).collect(),
        }
    }

    fn lookup_operating_period(
        data: &[NetexData],
        mut global_index: usize,
    ) -> Option<&UicOperatingPeriod> {
        for one_data in data {
            if global_index < one_data.operating_periods.len() {
                return Some(&one_data.operating_periods[global_index]);
            }
            global_index -= one_data.operating_periods.len()
        }
        None
    }
}

fn great_circle_distance(a: (f32, f32), b: (f32, f32)) -> f32 {
    use std::f32::consts;
    let a_lon = a.0 * consts::PI / 180.0;
    let a_lat = a.1 * consts::PI / 180.0;
    let b_lon = b.0 * consts::PI / 180.0;
    let b_lat = b.1 * consts::PI / 180.0;

    let diff_lon = (a_lon - b_lon).abs();
    let intermediate = a_lat.sin() * b_lat.sin() + a_lat.cos() * b_lat.cos() * diff_lon.cos();
    let angle = intermediate.acos();
    6371.009 * angle
}
