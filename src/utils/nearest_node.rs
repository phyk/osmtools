use kiddo::{ImmutableKdTree, SquaredEuclidean};
use proj::Proj;
use polars::prelude::*;

pub fn add_nearest_node_to_geo_df<'a>(geo_df: DataFrame, nodes_to_match: &DataFrame, target_crs: &str) -> DataFrame {
    let proj_to_m = Proj::new_known_crs(
        "EPSG:4326",
        target_crs,
        None,
    ).unwrap();
    let nodes_projected: Vec<[f64;2]> = nodes_to_match.column("lat").unwrap().f64().unwrap().into_iter().zip(nodes_to_match.column("long").unwrap().f64().unwrap().into_iter()).map(|(lat, long)| proj_to_m.convert((long.unwrap(), lat.unwrap())).unwrap()).map(|(a, b)| [a, b]).collect();
    let kdtree = ImmutableKdTree::new_from_slice(&nodes_projected);
    let (osm_id, dist): (Vec<u64>, Vec<f64>) = geo_df.column("lat").unwrap().f64().unwrap().into_iter().zip(geo_df.column("long").unwrap().f64().unwrap().into_iter()).map(|(lat, long)| {
        let point = proj_to_m.convert((long.unwrap(), lat.unwrap())).unwrap();
        let nearest_node = kdtree.nearest_one::<SquaredEuclidean>(&[point.0, point.1]);
        let matched_nearest_node = nodes_to_match.column("osm_id").unwrap().u64().unwrap().get(nearest_node.item as usize).unwrap();
        (matched_nearest_node, nearest_node.distance.sqrt())
    }).unzip();
    return geo_df.lazy().with_columns([Series::new("nearest_node_osm_id".into(), osm_id).lit(), Series::new("nearest_node_distance".into(), dist).lit()]).collect().unwrap();
}
