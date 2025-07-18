use std::error;

use geo::Point;
use kiddo::{ImmutableKdTree, SquaredEuclidean};
use polars::prelude::*;
use proj4rs;

pub fn add_nearest_node_to_geo_df<'a>(
    geo_df: DataFrame,
    nodes_to_match: &DataFrame,
    target_crs: u16,
) -> Result<DataFrame, Box<dyn error::Error>> {
    let proj_from = proj4rs::Proj::from_epsg_code(4326)?;
    let proj_to = proj4rs::Proj::from_epsg_code(target_crs)?;
    let mut nodes: Vec<Point> = nodes_to_match
        .column("lat")?
        .f64()?
        .into_iter()
        .zip(nodes_to_match.column("long")?.f64()?.into_iter())
        .map(|(lat, long)| Point::new(long.unwrap(), lat.unwrap()).to_radians())
        .collect();
    println!("Nodes: {:?}", nodes);
    nodes
        .iter_mut()
        .for_each(|p| proj4rs::transform::transform(&proj_from, &proj_to, p).unwrap());
    let nodes_arr: Vec<[f64; 2]> = nodes.iter().map(|p| [p.x(), p.y()]).collect();
    let kdtree = ImmutableKdTree::new_from_slice(&nodes_arr);
    let (id, dist): (Vec<u64>, Vec<f64>) = geo_df
        .column("lat")?
        .f64()?
        .into_iter()
        .zip(geo_df.column("long")?.f64()?.into_iter())
        .map(|(lat, long)| {
            let mut point = Point::new(long.unwrap(), lat.unwrap()).to_radians();
            proj4rs::transform::transform(&proj_from, &proj_to, &mut point).unwrap();
            println!("Point: {:?}", point);
            let nearest_node = kdtree.nearest_one::<SquaredEuclidean>(&[point.x(), point.y()]);
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
    fn test_adding_nodes_same_crs() -> Result<(), Box<dyn error::Error>> {
        let add_df = df![
            "lat" => [0.0, 1.0, 2.0],
            "long" => [0.0, 0.0, 0.0],
            "osm_id" => [1u64, 2u64, 3u64]
        ]
        .unwrap();
        let target_df = df![
            "lat" => [0.0, 0.0],
            "long" => [0.0, 1.0],
            "osm_id" => [0u64, 10u64]
        ]
        .unwrap();
        let result = add_nearest_node_to_geo_df(target_df.clone(), &add_df, &4326);
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
                assert_eq!(join_comp.shape().0, 2);
                assert_eq!(
                    df.column("nearest_node_distance")?.get(0).unwrap(),
                    polars::prelude::AnyValue::Float64(0.0)
                );
                assert_eq!(
                    df.column("nearest_node_distance")?.get(1).unwrap(),
                    polars::prelude::AnyValue::Float64(Point::new(0.0, 1.0).to_radians().y())
                );
            }
            Err(err) => panic!("No Error allowed in this test: {err:?}"),
        };
        Ok(())
    }

    #[test]
    fn test_adding_nodes_4839_crs() -> Result<(), Box<dyn error::Error>> {
        let add_df = df![
            "lat" => [ 50.9488246, 50.9498878, 50.9482893],
            "long" => [6.9117076, 6.9169238, 6.9202445],
            "osm_id" => [1u64, 2u64, 3u64]
        ]
        .unwrap();
        let target_df = df![
            "lat" => [50.9500121, 50.9481067],
            "long" => [6.9217811, 6.9141058],
            "osm_id" => [0u64, 10u64]
        ]
        .unwrap();
        let result = add_nearest_node_to_geo_df(target_df.clone(), &add_df, &4839);
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
                assert_eq!(join_comp.shape().0, 2);
                let mut row = df.get(0).unwrap();
                let target = vec![
                    polars::prelude::AnyValue::Float64(50.9500121),
                    polars::prelude::AnyValue::Float64(6.9217811),
                    polars::prelude::AnyValue::UInt64(0),
                    polars::prelude::AnyValue::UInt64(3),
                    polars::prelude::AnyValue::Float64(219.77669311702834),
                ];
                row.iter().zip(target.iter()).for_each(|(a, b)| {
                    assert_eq!(a, b);
                });
                row = df.get(1).unwrap();
                let target = vec![
                    polars::prelude::AnyValue::Float64(50.9481067),
                    polars::prelude::AnyValue::Float64(6.9141058),
                    polars::prelude::AnyValue::UInt64(10),
                    polars::prelude::AnyValue::UInt64(1),
                    polars::prelude::AnyValue::Float64(186.32441829502295),
                ];
                row.iter().zip(target.iter()).for_each(|(a, b)| {
                    assert_eq!(a, b);
                });
            }
            Err(err) => panic!("No Error allowed in this test: {err:?}"),
        };
        Ok(())
    }
}
