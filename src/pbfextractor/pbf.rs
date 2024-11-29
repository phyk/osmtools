use geo::{Contains, Polygon};
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
use osmpbfreader::{OsmObj, OsmPbfReader, Way};
use proj::Coord;

use super::metrics::{CostMetric, Distance_, EdgeFilter, NodeMetric, TagMetric};
use proj::Proj;
use std::cmp::Ordering;
use std::collections::hash_map::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::spawn;

pub type TagMetrics = Vec<Rc<dyn TagMetric<f64>>>;
pub type NodeMetrics = Vec<Rc<dyn NodeMetric<f64>>>;
pub type CostMetrics = Vec<Rc<dyn CostMetric<f64>>>;
pub type InternalMetrics = HashSet<String>;
pub type MetricIndices = BTreeMap<String, usize>;
#[derive(Debug)]
pub struct LoaderBuildError {
    source: String,
}

impl Error for LoaderBuildError {}
impl Display for LoaderBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Missing required field {}", self.source)
    }
}

pub struct Loader<Filter: EdgeFilter> {
    pbf_path: PathBuf,
    edge_filter: Filter,
    filter_geometry: Option<Polygon>,
    proj_to_m: Proj,
}

#[derive(Default)]
pub struct OsmLoaderBuilder<Filter: EdgeFilter> {
    pbf_path: Option<PathBuf>,
    edge_filter: Option<Filter>,
    filter_geometry: Option<Polygon>,
    target_crs: Option<String>,
}

#[allow(dead_code)]
impl<Filter: EdgeFilter> OsmLoaderBuilder<Filter> {
    pub fn pbf_path<VALUE: Into<PathBuf>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.pbf_path = Some(value.into());
        new
    }
    pub fn pbf_path_from_str<VALUE: Into<String>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.pbf_path = Some(Path::new(&value.into()).to_path_buf());
        new
    }
    pub fn edge_filter<VALUE: Into<Filter>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.edge_filter = Some(value.into());
        new
    }
    pub fn filter_geometry<VALUE: Into<Polygon>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.filter_geometry = Some(value.into());
        new
    }
    pub fn target_crs<VALUE: Into<String>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.target_crs = Some(value.into());
        new
    }
    pub fn build(&self) -> Result<Loader<Filter>, LoaderBuildError> {
        let target_crs = self
            .target_crs
            .as_ref()
            .expect("Requires CRS to be set for any calculation");
        let proj_to_m = Proj::new_known_crs("EPSG:4326", &target_crs, None)
            .expect("Error in creation of Projection");

        Ok(Loader {
            pbf_path: match self.pbf_path {
                Some(ref value) => Clone::clone(value),
                None => {
                    return Err(LoaderBuildError {
                        source: "pbf_path".into(),
                    })
                }
            },
            edge_filter: match self.edge_filter {
                Some(ref value) => Clone::clone(value),
                None => {
                    return Err(LoaderBuildError {
                        source: "edge_filter".into(),
                    })
                }
            },
            filter_geometry: Clone::clone(&self.filter_geometry),
            proj_to_m: proj_to_m,
        })
    }
}

#[allow(clippy::too_many_arguments)]
impl<Filter: EdgeFilter> Loader<Filter> {
    /// Loads the graph from a pbf file.
    pub fn load_graph(&self) -> (Vec<Node>, Vec<Edge>) {
        println!(
            "Extracting data out of: {}",
            self.pbf_path
                .to_str()
                .expect("Path could not be converted to string")
        );
        let fs = File::open(self.pbf_path.as_path()).unwrap();
        let mut reader = OsmPbfReader::new(fs);

        let (id_sender, id_receiver) = channel();
        let set_receiver = self.collect_node_ids(id_receiver);

        let mut edges: Vec<Edge> = reader
            .par_iter()
            .flat_map(|obj| {
                if let Ok(OsmObj::Way(w)) = obj {
                    self.process_way(&w, &id_sender)
                } else {
                    Vec::new()
                }
            })
            .collect();
        println!("Collected {} edges", edges.len());
        reader.rewind().expect("Can't rewind pbf file!");
        drop(id_sender);

        let id_set = set_receiver.recv().expect("Did not get node ids");
        let mut skipped_nodes = 0;

        let mut nodes: Vec<Node> = reader
            .par_iter()
            .filter_map(|obj| {
                if let Ok(OsmObj::Node(n)) = obj {
                    if id_set.contains(&n.id) {
                        let lat = f64::from(n.decimicro_lat) / 10_000_000.0;
                        let lng = f64::from(n.decimicro_lon) / 10_000_000.0;
                        let point = geo::Point::new(lng, lat);
                        if self
                            .filter_geometry
                            .as_ref()
                            .is_some_and(|f| !f.contains(&point))
                        {
                            skipped_nodes += 1;
                            None
                        } else {
                            Some(Node::new(n.id.0 as usize, lat, lng))
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        println!("Collected {} nodes", nodes.len());
        if self.filter_geometry.is_some() {
            println!("Filtered {} nodes", skipped_nodes);
            let map: HashMap<OsmNodeId, (usize, &Node)> =
                nodes.iter().enumerate().map(|n| (n.1.osm_id, n)).collect();
            let mut edges_replace: Vec<Edge> = vec![];
            let num_edges = edges.len();
            for edge in edges {
                if map.contains_key(&edge.source) & map.contains_key(&edge.dest) {
                    edges_replace.push(edge);
                }
            }
            println!("Filtered {} edges", num_edges - edges_replace.len());
            edges = edges_replace;
        }
        println!("Num Edges {}", edges.len());

        println!("Calculating Metrics");

        self.rename_node_ids_and_calculate_node_metrics(&mut nodes, &mut edges);

        println!("Deleting duplicate and dominated edges");

        self.delete_duplicate_edges(&mut edges);
        edges = self.delete_dominated_edges(edges);

        println!("{} edges left", edges.len());
        (nodes, edges)
    }

    fn collect_node_ids(
        &self,
        ids: Receiver<osmpbfreader::NodeId>,
    ) -> Receiver<HashSet<osmpbfreader::NodeId>> {
        let (send, recv) = channel();

        spawn(move || {
            let mut set = HashSet::new();
            for id in ids {
                set.insert(id);
            }
            send.send(set)
                .expect("Cannot send node ids back to main thread");
        });
        recv
    }

    fn process_way(&self, w: &Way, id_sender: &Sender<osmpbfreader::NodeId>) -> Vec<Edge> {
        let mut edges = Vec::new();
        if self.edge_filter.is_invalid(&w.tags) {
            return edges;
        }

        let is_one_way = self.is_one_way(w);
        for (index, node) in w.nodes[0..(w.nodes.len() - 1)].iter().enumerate() {
            id_sender.send(*node).expect("could not send id to id set");
            let edge = Edge::new(
                node.0 as NodeId,
                w.nodes[index + 1].0 as NodeId,
            );
            edges.push(edge);
            if !is_one_way {
                let edge = Edge::new(
                    w.nodes[index + 1].0 as NodeId,
                    node.0 as NodeId
                );
                edges.push(edge);
            }
        }

        id_sender
            .send(*w.nodes.last().unwrap())
            .expect("could not send id to id set");
        edges
    }
    fn is_one_way(&self, way: &Way) -> bool {
        let one_way = way.tags.get("oneway");
        let highway = way.tags.get("highway");
        let junction = way.tags.get("junction");
        match one_way.map(smartstring::SmartString::as_ref) {
            Some("yes") | Some("true") => true,
            Some("no") | Some("false") => false,
            _ => {
                highway.map(|h| h == "motorway").unwrap_or(false)
                    || junction
                        .map(|j| j == "roundabout" || j == "circular")
                        .unwrap_or(false)
            }
        }
    }

    fn rename_node_ids_and_calculate_node_metrics(
        &self,
        nodes: &mut [Node],
        edges: &mut [Edge],
    ) {
        let map: HashMap<OsmNodeId, (usize, &Node)> =
            nodes.iter().enumerate().map(|n| (n.1.osm_id, n)).collect();
        for e in edges.iter_mut() {
            let (source_id, source) = map[&e.source];
            let (dest_id, dest) = map[&e.dest];
            if source_id == 0 {
                print!("{} {} {} - ", source.osm_id, source.lat, source.long)
            }
            e.source = source_id;
            e.dest = dest_id;
            
            e.dist = Distance_.calc(source, dest, &self.proj_to_m).expect("Cannot calculate distance");
        }
    }

    fn delete_duplicate_edges(&self, edges: &mut Vec<Edge>) {
        edges.sort_by(|e1, e2| {
            let mut result = e1.source.cmp(&e2.source);
            if result == Ordering::Equal {
                result = e1.dest.cmp(&e2.dest);
            }
            if result == Ordering::Equal {
                result = e1.dist.partial_cmp(&e2.dist).expect("Failure in comparing values");
            }
            result
        });
        edges.dedup();
    }

    fn delete_dominated_edges(&self, edges: Vec<Edge>) -> Vec<Edge> {
        let mut indices = ::std::collections::BTreeSet::new();
        for i in 1..edges.len() {
            let first = &edges[i - 1];
            let second = &edges[i];
            if !(first.source == second.source && first.dest == second.dest) {
                continue;
            }
            if first.dist <= second.dist
            {
                indices.insert(i);
            }
        }
        edges
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !indices.contains(i))
            .map(|(_, e)| e)
            .collect()
    }
}

pub type NodeId = usize;
pub type OsmNodeId = usize;
pub type Latitude = f64;
pub type Longitude = f64;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Node {
    pub osm_id: OsmNodeId,
    pub lat: Latitude,
    pub long: Longitude,
}

impl Coord<f64> for Node {
    fn x(&self) -> f64 {
        self.long
    }

    fn y(&self) -> f64 {
        self.lat
    }

    fn from_xy(x: f64, y: f64) -> Self {
        Self {
            osm_id: 0,
            long: x,
            lat: y,
        }
    }
}

impl Node {
    pub fn new(osm_id: OsmNodeId, lat: Latitude, long: Longitude) -> Node {
        Node { osm_id, lat, long }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Edge {
    pub source: NodeId,
    pub dest: NodeId,
    pub dist: f64,
}

impl Edge {
    pub fn new(source: NodeId, dest: NodeId) -> Edge {
        let dist = -1.0;
        Edge {
            source,
            dest,
            dist,
        }
    }
}

impl PartialEq for Edge {
    fn eq(&self, rhs: &Self) -> bool {
        self.source == rhs.source
            && self.dest == rhs.dest
            && self.dist == rhs.dist
    }
}
