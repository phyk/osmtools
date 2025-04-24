use std::{fs::File, io::BufReader};
use kiddo::ImmutableKdTree;
use log::warn;
use proj::Proj;
use polars::prelude::{DataFrame, col};

use crate::pbfextractor::pbf::{LoaderBuildError, Node};

pub fn add_nearest_node_to_geo_df(geo_df: &DataFrame, nodes_to_match: &DataFrame, target_crs: &str) -> DataFrame {
    let proj_to_m = Proj::new_known_crs(
        "EPSG:4326",
        target_crs,
        None,
    );
    let nodes_projected: Vec[f64; 2] = nodes_to_match.lazy().select(["lat", "lon"]).unwrap()
    .iter()
    .map(|x, y| proj_to_m.convert(x, y)).unwrap().into().collect();
    let kdtree = ImmutableKdTree::new_from_slice(&nodes_projected);
    return geo_df.with_column(
        col("lat")
            .zip(col("lon"))
            .apply(|(lat, lon)| {
                let point = proj_to_m.convert(lat, lon);
                let nearest_node = kdtree.nearest_one::<SquaredEuclidean>(&[point.x(), point.y()]);
                let matched_nearest_node = nodes_to_match.get::<usize>(nearest_node.item as usize).unwrap();
                (osm_nearest_node.osm_id, nearest_node.distance)
            })
    )
}

impl NodeMatcher {
    pub fn match_nodes(&self, input_nodes: &Vec<Node>) -> Vec<Poi> {
        info!(
            "Searching closest node for {} nodes",
            input_nodes.len()
        );

        let nodes: Vec<Poi> = reader
            .par_iter()
            .filter_map(|obj| {
                if let Ok(OsmObj::Node(n)) = obj {
                    let lat = f64::from(n.decimicro_lat) / 10_000_000.0;
                    let lng = f64::from(n.decimicro_lon) / 10_000_000.0;
                    let point = geo::Point::new(lng, lat);
                    let point_convert = self.proj_to_m.convert(point).unwrap();
                    let nearest_node = self
                        .kdtree
                        .nearest_one::<SquaredEuclidean>(&[point_convert.x(), point_convert.y()]);
                    let osm_nearest_node: &super::pbf::Node = self
                        .nodes_to_match
                        .get::<usize>(nearest_node.item as usize)
                        .expect("Impossible, all nodes have to exist");
                    if self
                        .filter_geometry
                        .as_ref()
                        .is_some_and(|f| !f.contains(&point))
                    {
                        skipped_nodes += 1;
                        None
                    } else {
                        match identify_type(&n) {
                            Some(v) => Some(Poi::new(
                                n.id.0 as usize,
                                lat,
                                lng,
                                osm_nearest_node.osm_id,
                                nearest_node.distance,
                                v,
                            )),
                            None => None,
                        }
                    }
                } else {
                    None
                }
            })
            .collect();

        info!("Collected {} nodes", nodes.len());
        info!("Calculating Metrics");

        nodes
    }
}
