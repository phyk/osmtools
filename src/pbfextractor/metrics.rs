/*
Pbfextractor creates graph files for the cycle-routing projects from pbf and srtm data
Copyright (C) 2018  Florian Barth

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
use super::pbf::{MetricIndices, Node};
use super::units::*;

use geo::{Distance, Euclidean};
use geo_types::Point;
use osmpbfreader::Tags;
use proj::{Coord, Proj};
use smartstring::{LazyCompact, SmartString};

use std::rc::Rc;

#[derive(Debug)]
pub enum MetricError {
    UnknownMetric,
    NonFiniteTime(f64, f64),
}

pub type MetricResult<T> = Result<T, MetricError>;

pub trait Metric {
    fn name(&self) -> String;
}

macro_rules! metric {
    ($t:ty) => {
        impl Metric for $t {
            fn name(&self) -> String {
                stringify!($t).to_owned()
            }
        }
    };
}

pub trait TagMetric<T>: Metric {
    fn calc(&self, tags: &Tags) -> MetricResult<T>;
}

pub trait NodeMetric<T>: Metric {
    fn calc(&self, source: &Node, target: &Node, proj_to_m: &Proj) -> MetricResult<T>;
}

pub trait CostMetric<T>: Metric {
    fn calc(&self, costs: &[f64], map: &MetricIndices) -> MetricResult<T>;
}

fn bounded_speed(tags: &Tags, driver_max: f64) -> MetricResult<KilometersPerHour> {
    let street_type = tags.get("highway").map(smartstring::alias::String::as_ref);
    let tag_speed = match street_type {
        Some("motorway") | Some("trunk") => driver_max,
        Some("primary") => 100.0,
        Some("secondary") | Some("trunk_link") => 80.0,
        Some("motorway_link")
        | Some("primary_link")
        | Some("secondary_link")
        | Some("tertiary")
        | Some("tertiary_link") => 70.0,
        Some("service") => 30.0,
        Some("living_street") => 5.0,
        _ => 50.0,
    };

    let max_speed_tag = tags.get("maxspeed");
    let max_speed = match max_speed_tag.map(smartstring::alias::String::as_ref) {
        Some("none") => Some(driver_max),
        Some("walk") | Some("DE:walk") => Some(10.0),
        Some("living_street") | Some("DE:living_street") => Some(10.0),
        Some(s) => s.parse().ok(),
        None => None,
    };

    let speed = match max_speed {
        Some(s) if s > 0.0 && s <= driver_max => s,
        _ => tag_speed.min(driver_max),
    };
    Ok(KilometersPerHour(speed))
}

#[allow(dead_code)]
pub struct Distance_;
metric!(Distance_);

impl NodeMetric<Meters> for Distance_ {
    fn calc(&self, source: &Node, target: &Node, proj_to_m: &Proj) -> MetricResult<Meters> {
        let source_point = Point::from_xy(source.x(), source.y());
        let target_point = Point::from_xy(target.x(), target.y());
        let source_proj = proj_to_m.convert(source_point).unwrap();
        let target_proj = proj_to_m.convert(target_point).unwrap();
        Ok(Meters(Euclidean::distance(source_proj, target_proj)))
    }
}

#[allow(dead_code)]
pub struct TravelTime<D: Metric, S: Metric> {
    distance: Rc<D>,
    speed: Rc<S>,
}

impl<D, S> Metric for TravelTime<D, S>
where
    D: Metric,
    S: Metric,
{
    fn name(&self) -> String {
        format!(
            "TravelTime: {} / {}",
            self.distance.name(),
            self.speed.name()
        )
    }
}

impl<D, S> TravelTime<D, S>
where
    D: Metric,
    S: Metric,
{
    pub fn new(distance: Rc<D>, speed: Rc<S>) -> TravelTime<D, S> {
        TravelTime { distance, speed }
    }
}

impl<D, S> CostMetric<Seconds> for TravelTime<D, S>
where
    D: Metric,
    S: Metric,
{
    fn calc(&self, costs: &[f64], map: &MetricIndices) -> MetricResult<Seconds> {
        let dist_index = *map
            .get(&self.distance.name())
            .ok_or(MetricError::UnknownMetric)?;
        let speed_index = *map
            .get(&self.speed.name())
            .ok_or(MetricError::UnknownMetric)?;

        let dist = Meters(costs[dist_index]);
        let speed = KilometersPerHour(costs[speed_index]);
        let time = dist / MetersPerSecond::from(speed);

        if time.0.is_finite() {
            Ok(time)
        } else {
            Err(MetricError::NonFiniteTime(dist.0, speed.0))
        }
    }
}

impl<T> CostMetric<f64> for T
where
    T: CostMetric<Seconds>,
{
    fn calc(&self, costs: &[f64], map: &MetricIndices) -> MetricResult<f64> {
        CostMetric::<Seconds>::calc(self, costs, map).map(|c| c.0)
    }
}

impl<T> NodeMetric<f64> for T
where
    T: NodeMetric<Meters>,
{
    fn calc(&self, source: &Node, target: &Node, proj_to_m: &Proj) -> MetricResult<f64> {
        NodeMetric::<Meters>::calc(self, source, target, proj_to_m).map(|c| c.0)
    }
}

impl<T> TagMetric<f64> for T
where
    T: TagMetric<KilometersPerHour>,
{
    fn calc(&self, tags: &Tags) -> MetricResult<f64> {
        TagMetric::<KilometersPerHour>::calc(self, tags).map(|c| c.0)
    }
}

#[allow(dead_code)]
pub struct UnsuitDistMetric<U, D> {
    distance: Rc<D>,
    unsuitability: Rc<U>,
}

impl<U, D> Metric for UnsuitDistMetric<U, D>
where
    D: Metric,
    U: Metric,
{
    fn name(&self) -> String {
        format!(
            "UnsuitDistMetric: {} / {}",
            self.distance.name(),
            self.unsuitability.name()
        )
    }
}

impl<D, U> UnsuitDistMetric<U, D>
where
    D: Metric,
    U: Metric,
{
    #[allow(dead_code)]
    pub fn new(distance: Rc<D>, unsuitability: Rc<U>) -> Self {
        UnsuitDistMetric {
            distance,
            unsuitability,
        }
    }
}

impl<D, U> CostMetric<f64> for UnsuitDistMetric<U, D>
where
    D: Metric,
    U: Metric,
{
    fn calc(&self, costs: &[f64], map: &MetricIndices) -> MetricResult<f64> {
        let dist_index = *map
            .get(&self.distance.name())
            .ok_or(MetricError::UnknownMetric)?;
        let unsuitability_index = *map
            .get(&self.unsuitability.name())
            .ok_or(MetricError::UnknownMetric)?;

        let dist = costs[dist_index];
        let unsuitability = costs[unsuitability_index];
        Ok(unsuitability * dist)
    }
}

#[allow(dead_code)]
pub struct BicycleUnsuitability;
metric!(BicycleUnsuitability);

impl TagMetric<f64> for BicycleUnsuitability {
    fn calc(&self, tags: &Tags) -> MetricResult<f64> {
        let bicycle_tag = tags.get("bicycle");
        if tags.get("cycleway").is_some()
            || bicycle_tag.is_some() && bicycle_tag != Some(&SmartString::<LazyCompact>::from("no"))
        {
            return Ok(0.5);
        }

        let side_walk: Option<&str> = tags.get("sidewalk").map(smartstring::alias::String::as_ref);
        if side_walk == Some("yes") {
            return Ok(1.0);
        }

        let street_type = tags.get("highway").map(smartstring::alias::String::as_ref);
        let unsuitability = match street_type {
            Some("primary") => 5.0,
            Some("primary_link") => 5.0,
            Some("secondary") => 4.0,
            Some("secondary_link") => 4.0,
            Some("tertiary") => 3.0,
            Some("tertiary_link") => 3.0,
            Some("road") => 3.0,
            Some("bridleway") => 3.0,
            Some("unclassified") => 2.0,
            Some("residential") => 2.0,
            Some("traffic_island") => 2.0,
            Some("living_street") => 1.0,
            Some("service") => 1.0,
            Some("track") => 1.0,
            Some("platform") => 1.0,
            Some("pedestrian") => 1.0,
            Some("path") => 1.0,
            Some("footway") => 1.0,
            Some("cycleway") => 0.5,
            _ => 6.0,
        };
        Ok(unsuitability)
    }
}

#[allow(dead_code)]
pub struct EdgeCount;
metric!(EdgeCount);

impl TagMetric<f64> for EdgeCount {
    fn calc(&self, _: &Tags) -> MetricResult<f64> {
        Ok(1.0)
    }
}

pub trait EdgeFilter {
    fn is_invalid(&self, tags: &Tags) -> bool;
}

#[allow(dead_code)]
pub struct BicycleEdgeFilter;

impl EdgeFilter for BicycleEdgeFilter {
    fn is_invalid(&self, tags: &Tags) -> bool {
        let bicycle_tag = tags.get("bicycle");
        if bicycle_tag == Some(&SmartString::<LazyCompact>::from("no")) {
            return true;
        }
        if tags.get("cycleway").is_some()
            || bicycle_tag.is_some() && bicycle_tag != Some(&SmartString::<LazyCompact>::from("no"))
        {
            return false;
        }

        let street_type = tags.get("highway").map(smartstring::alias::String::as_ref);
        let side_walk: Option<&str> = tags.get("sidewalk").map(smartstring::alias::String::as_ref);
        let has_side_walk: bool = match side_walk {
            Some(s) => s != "no",
            None => false,
        };
        if has_side_walk {
            return false;
        }
        matches!(
            street_type,
            Some("motorway")
                | Some("motorway_link")
                | Some("trunk")
                | Some("trunk_link")
                | Some("proposed")
                | Some("steps")
                | Some("elevator")
                | Some("corridor")
                | Some("raceway")
                | Some("rest_area")
                | Some("construction")
                | Some("service")
                | None
        )
    }
}
#[allow(dead_code)]
pub struct CarEdgeFilter;

impl EdgeFilter for CarEdgeFilter {
    fn is_invalid(&self, tags: &Tags) -> bool {
        let street_type = tags.get("highway").map(smartstring::alias::String::as_ref);
        matches!(
            street_type,
            Some("footway")
                | Some("bridleway")
                | Some("steps")
                | Some("path")
                | Some("cycleway")
                | Some("track")
                | Some("proposed")
                | Some("construction")
                | Some("pedestrian")
                | Some("rest_area")
                | Some("elevator")
                | Some("raceway")
                | Some("service")
                | None
        )
    }
}
