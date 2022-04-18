#[derive(Default)]
pub struct ScheduledStopPoint {
    pub id: String,
    pub short_name: String,
    pub long: f32,
    pub lat: f32,
}

#[derive(Default)]
pub struct StopPointInJourneyPattern {
    pub id: String,
    pub scheduled_stop_point: String,
}

#[derive(Default)]
pub struct PointsInSequence {
    /// refs for scheduled point stops
    pub stops: Vec<StopPointInJourneyPattern>,
}

#[derive(Default)]
pub struct TimetabledPassingTime {
    pub stop_point_in_journey_pattern: String,
    pub arrival: String,
    pub departure: String,
}

#[derive(Default)]
pub struct ServiceJourney {
    pub passing_times: Vec<TimetabledPassingTime>,
}

#[derive(Default)]
pub struct NetexData {
    pub scheduled_stop_points: Vec<ScheduledStopPoint>,
    pub points_in_squence: Vec<PointsInSequence>,
    pub service_journeys: Vec<ServiceJourney>,
}

impl NetexData {
    pub fn from_xml(
        mut read: impl std::io::Read,
        size: usize,
    ) -> Result<NetexData, Box<dyn std::error::Error>> {
        let mut data = NetexData::default();
        let mut buf = Vec::<u8>::with_capacity(size);
        read.read_to_end(&mut buf)?;
        let text = unsafe { String::from_utf8_unchecked(buf) };
        let document = roxmltree::Document::parse(&text)?;

        let nodes: Result<Vec<ScheduledStopPoint>, Box<dyn std::error::Error>> = document
            .descendants()
            .filter(|node| node.tag_name().name() == "ScheduledStopPoint")
            .map(|node| NetexData::parse_scheduled_stop_point(&node))
            .collect();
        data.scheduled_stop_points = nodes?;

        let points: Vec<PointsInSequence> = document
            .descendants()
            .filter(|node| node.tag_name().name() == "pointsInSequence")
            .map(|node| NetexData::parse_points_in_sequence(&node))
            .collect();
        data.points_in_squence = points;

        let journeys: Vec<ServiceJourney> = document
            .descendants()
            .filter(|node| node.tag_name().name() == "ServiceJourney")
            .map(|node| NetexData::parse_service_journey(&node))
            .collect();
        data.service_journeys = journeys;
        Ok(data)
    }

    fn parse_scheduled_stop_point(
        node: &roxmltree::Node,
    ) -> Result<ScheduledStopPoint, Box<dyn std::error::Error>> {
        let mut result = ScheduledStopPoint {
            id: node.attribute("id").unwrap_or_default().to_owned(),
            ..ScheduledStopPoint::default()
        };
        for child in node.descendants() {
            match child.tag_name().name() {
                "ShortName" => result.short_name = child.text().unwrap_or_default().to_owned(),
                "Longitude" => result.long = child.text().unwrap_or_default().parse()?,
                "Latitude" => result.lat = child.text().unwrap_or_default().parse()?,
                _ => {}
            }
        }
        Ok(result)
    }

    fn parse_points_in_sequence(node: &roxmltree::Node) -> PointsInSequence {
        let mut result = PointsInSequence::default();
        for sub_node in node.descendants() {
            if sub_node.tag_name().name() != "StopPointInJourneyPattern" {
                continue;
            }
            let mut stop = StopPointInJourneyPattern {
                id: sub_node.attribute("id").unwrap_or_default().to_owned(),
                ..StopPointInJourneyPattern::default()
            };
            stop.scheduled_stop_point = sub_node
                .descendants()
                .find(|child| child.tag_name().name() == "ScheduledStopPointRef")
                .map(|node| node.attribute("ref").unwrap_or_default().to_owned())
                .unwrap_or_default();
            result.stops.push(stop);
        }
        result
    }

    fn parse_service_journey(node: &roxmltree::Node) -> ServiceJourney {
        let mut result = ServiceJourney::default();
        let passing_times_node = node
            .descendants()
            .find(|node| node.tag_name().name() == "passingTimes")
            .expect("ServiceJouney tag has no passing times");
        for timetabled in passing_times_node
            .descendants()
            .filter(|node| node.tag_name().name() == "TimetabledPassingTime")
        {
            let mut timetabled_passing_time = TimetabledPassingTime::default();
            for child in timetabled.descendants() {
                match child.tag_name().name() {
                    "StopPointInJourneyPatternRef" => {
                        timetabled_passing_time.stop_point_in_journey_pattern =
                            child.attribute("ref").unwrap_or_default().to_owned();
                    }
                    "ArrivalTime" => {
                        timetabled_passing_time.arrival =
                            child.text().unwrap_or_default().to_owned();
                    }
                    "DepartureTime" => {
                        timetabled_passing_time.departure =
                            child.text().unwrap_or_default().to_owned();
                    }
                    _ => {}
                }
            }
            result.passing_times.push(timetabled_passing_time);
        }
        result
    }
}
