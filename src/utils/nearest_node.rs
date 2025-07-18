use std::error;

use kiddo::{ImmutableKdTree, SquaredEuclidean};
use polars::prelude::*;
use proj::Proj;

pub fn add_nearest_node_to_geo_df<'a>(
    geo_df: DataFrame,
    nodes_to_match: &DataFrame,
    target_crs: &str,
) -> Result<DataFrame, Box<dyn error::Error>> {
    let proj_to_m = Proj::new_known_crs("EPSG:4326", target_crs, None)?;
    let nodes_projected: Vec<[f64; 2]> = nodes_to_match
        .column("lat")?
        .f64()?
        .into_iter()
        .zip(nodes_to_match.column("long")?.f64()?.into_iter())
        .map(|(lat, long)| proj_to_m.convert((long.unwrap(), lat.unwrap())).unwrap())
        .map(|(a, b)| [a, b])
        .collect();
    let kdtree = ImmutableKdTree::new_from_slice(&nodes_projected);
    let (id, dist): (Vec<u64>, Vec<f64>) = geo_df
        .column("lat")?
        .f64()?
        .into_iter()
        .zip(geo_df.column("long")?.f64()?.into_iter())
        .map(|(lat, long)| {
            let point = proj_to_m.convert((long.unwrap(), lat.unwrap())).unwrap();
            let nearest_node = kdtree.nearest_one::<SquaredEuclidean>(&[point.0, point.1]);
            let matched_nearest_node = nodes_to_match
                .column("osm_id")
                .unwrap()
                .u64()
                .unwrap()
                .get(nearest_node.item as usize)
                .unwrap();
            (matched_nearest_node, nearest_node.distance.sqrt())
        })
        .unzip();
    let series_nearest_node = Series::new("nearest_node_osm_id".into(), id);
    let series_nearest_distance = Series::new("nearest_node_distance".into(), dist);
    return geo_df
        .lazy()
        .with_columns([series_nearest_node.lit(), series_nearest_distance.lit()])
        .collect()
        .map_err(|e| e.into());
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    #[test]
    fn test_adding_nodes() -> Result<(), Box<dyn error::Error>> {
        let add_df = df![
            "lat" => [0.0, 1.0, 2.0],
            "long" => [0.0, 0.0, 0.0],
            "osm_id" => [1, 2, 3]
        ]
        .unwrap();
        let target_df = df![
            "lat" => [0.0],
            "long" => [0.0],
            "osm_id" => [0]
        ]
        .unwrap();
        let result = add_nearest_node_to_geo_df(target_df.clone(), &add_df, "EPSG:4326");
        match result {
            Ok(df) => {
                let join_comp = df
                    .join(
                        &target_df,
                        ["osm_id"],
                        ["osm_id"],
                        JoinArgs::new(JoinType::Inner),
                        None,
                    )
                    .unwrap();
                assert_eq!(join_comp.size(), 1);
            }
            Err(err) => panic!("No Error allowed in this test: {err:?}"),
        };
        Ok(())
    }
}
