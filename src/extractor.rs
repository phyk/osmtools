use crate::pbfextractor::metrics::{
    BicycleEdgeFilter, CarEdgeFilter, EdgeFilter, WalkingEdgeFilter,
};
use crate::pbfextractor::node_pbf::PoiLoaderBuilder;
use crate::pbfextractor::pbf::{Loader, OsmLoaderBuilder};
use crate::struct_to_dataframe;
use geo::{LineString, Polygon};
use log::info;
use polars::frame::DataFrame;
use std::fs::File;
use std::io::{self, BufWriter, Error, ErrorKind};
use std::path::{Path, PathBuf};

fn check_pbf_archives(
    city_name: &str,
    archive_path: &str,
    download: bool,
) -> Result<PathBuf, Error> {
    let pbf_path;
    if download {
        pbf_path = crate::download::download(&city_name.into(), &archive_path.into())
            .expect("Error in Download");
    } else {
        pbf_path =
            Path::new(archive_path).join(Path::new(&(city_name.to_lowercase() + ".osm.pbf")));
        if !pbf_path.exists() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("FileNotFoundError: {}", pbf_path.to_str().unwrap()),
            ));
        }
    }
    return Ok(pbf_path);
}

fn get_edge_outpath(outpath: &str, city_name: &str, network_type: &str) -> String {
    let mut outpath_edges = get_outpath(outpath, city_name, network_type);
    outpath_edges.push_str("_edges.parquet");
    outpath_edges
}

fn get_node_outpath(outpath: &str, city_name: &str, network_type: &str) -> String {
    let mut outpath_node = get_outpath(outpath, city_name, network_type);
    outpath_node.push_str("_nodes.parquet");
    outpath_node
}

fn get_outpath(outpath: &str, city_name: &str, network_type: &str) -> String {
    let mut outpath = outpath.to_owned();
    outpath.push_str("/");
    outpath.push_str(&*city_name.to_lowercase());
    outpath.push_str("_");
    outpath.push_str(network_type);
    outpath
}

pub fn _load_osm_pois(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    nodes_to_match_path: Option<&str>,
    nodes_to_match_df: Option<&DataFrame>,
    outpath: &str,
    download: bool,
) -> DataFrame {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");

    // Then give kdtree to PoiLoader, or create it inside of PoiLoader from nodes from csv
    // Search nearest neighbor in loop in PoiLoader
    let mut osm_loader_builder = PoiLoaderBuilder::default();

    osm_loader_builder
        .target_crs(4839u16)
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path);
    match nodes_to_match_df {
        Some(df) => {
            osm_loader_builder.nodes_to_match_polars(df.clone());
        }
        _ => (),
    }
    match nodes_to_match_path {
        Some(path) => {
            osm_loader_builder.nodes_to_match_parquet(path);
        }
        None => (),
    }
    let osm_loader = osm_loader_builder.build().expect("Parameter missing");
    let outpath_nodes = get_node_outpath(outpath, city_name, "pois");

    let nodes = osm_loader.load_graph();
    let output_file_nodes = File::create(outpath_nodes).unwrap();
    let node_writer = BufWriter::new(output_file_nodes);

    let parquet_writer = polars_io::parquet::write::ParquetWriter::new(node_writer);
    let mut df = struct_to_dataframe!(
        nodes,
        [
            osm_id,
            lat,
            long,
            nearest_osm_node,
            dist_to_nearest,
            poi_type
        ]
    )
    .unwrap();
    parquet_writer.finish(&mut df).unwrap();
    df
}

pub fn _load_osm_walking(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<WalkingEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(WalkingEdgeFilter)
        .target_crs(4839u16)
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .reverse_edges(true)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "walking");
    let outpath_nodes = get_node_outpath(outpath, city_name, "walking");

    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(&osm_loader, &outpath_edges, &outpath_nodes).expect("Error in writing")
}
pub fn _load_osm_cycling(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    reverse_edges: &bool,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<BicycleEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(BicycleEdgeFilter)
        .target_crs(4839u16)
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .reverse_edges(*reverse_edges)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "cycling");
    let outpath_nodes = get_node_outpath(outpath, city_name, "cycling");
    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(&osm_loader, &outpath_edges, &outpath_nodes).expect("Error in writing")
}
pub fn _load_osm_driving(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<CarEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(CarEdgeFilter)
        .target_crs(4839u16)
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "driving");
    let outpath_nodes = get_node_outpath(outpath, city_name, "driving");
    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(&osm_loader, &outpath_edges, &outpath_nodes).expect("Error in writing")
}

fn write_graph<T: EdgeFilter>(
    l: &Loader<T>,
    outpath_edges: &str,
    outpath_nodes: &str,
) -> Result<(DataFrame, DataFrame), io::Error> {
    let output_file_edges = File::create(outpath_edges).unwrap();
    let output_file_nodes = File::create(outpath_nodes).unwrap();
    let edge_writer = BufWriter::new(output_file_edges);
    let node_writer = BufWriter::new(output_file_nodes);

    let (nodes, edges) = l.load_graph();

    info!("Writing edges to {}", outpath_edges);

    let mut parquet_writer = polars_io::parquet::write::ParquetWriter::new(edge_writer);
    let mut df_edges: polars::prelude::DataFrame =
        struct_to_dataframe!(edges, [source_osm, dest_osm, length]).unwrap();
    parquet_writer.finish(&mut df_edges).unwrap();

    info!("Writing nodes to {}", outpath_nodes);
    parquet_writer = polars_io::parquet::write::ParquetWriter::new(node_writer);
    let mut df_nodes = struct_to_dataframe!(nodes, [osm_id, lat, long]).unwrap();
    parquet_writer.finish(&mut df_nodes).unwrap();
    Ok((df_nodes, df_edges))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_test_osm_walking() {
        let bounding_box = vec![
            (3.22183, 51.20391),
            (3.23663, 51.20391),
            (3.23663, 51.20887),
            (3.22183, 51.20887),
            (3.22183, 51.20391),
        ];
        let (nodes, edges) =
            _load_osm_walking("Bruegge", bounding_box.clone(), "data", "test", false);
        assert_eq!(nodes.shape(), (1813, 3));
        assert_eq!(edges.shape(), (4032, 3));
    }

    #[test]
    fn integration_test_osm_cycling() {
        let bounding_box = vec![
            (3.22183, 51.20391),
            (3.23663, 51.20391),
            (3.23663, 51.20887),
            (3.22183, 51.20887),
            (3.22183, 51.20391),
        ];
        let (nodes, edges) = _load_osm_cycling(
            "Bruegge",
            bounding_box.clone(),
            &false,
            "data",
            "test",
            false,
        );
        assert_eq!(nodes.shape(), (1653, 3));
        assert_eq!(edges.shape(), (3325, 3));
    }

    #[test]
    fn integration_test_osm_driving() {
        let bounding_box = vec![
            (3.22183, 51.20391),
            (3.23663, 51.20391),
            (3.23663, 51.20887),
            (3.22183, 51.20887),
            (3.22183, 51.20391),
        ];
        let (nodes, edges) =
            _load_osm_driving("Bruegge", bounding_box.clone(), "data", "test", false);
        assert_eq!(nodes.shape(), (470, 3));
        assert_eq!(edges.shape(), (659, 3));
    }

    #[test]
    fn integration_test_osm_pois() {
        let bounding_box = vec![
            (3.22183, 51.20391),
            (3.23663, 51.20391),
            (3.23663, 51.20887),
            (3.22183, 51.20887),
            (3.22183, 51.20391),
        ];
        let result = _load_osm_pois(
            "Bruegge",
            bounding_box,
            "data",
            Some("test/bruegge_poitest_walking_nodes.parquet"),
            None,
            "test",
            false,
        );
        assert_eq!(result.shape(), (215, 6));
    }
}
