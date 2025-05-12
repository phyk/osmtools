use crate::pbfextractor::metrics::{
    BicycleEdgeFilter, CarEdgeFilter, Distance_, EdgeFilter, NodeMetric, WalkingEdgeFilter,
};
use crate::pbfextractor::node_pbf::PoiLoaderBuilder;
use crate::pbfextractor::pbf::{Loader, OsmLoaderBuilder};
use crate::pbfextractor::units::Meters;
use geo::{LineString, Polygon};
use h3o::{LatLng, Resolution};
use polars::frame::DataFrame;
use proj::Coord;
use crate::struct_to_dataframe;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
fn get_mapping_outpath(outpath: &str, city_name: &str, network_type: &str) -> String {
    let mut outpath_node = get_outpath(outpath, city_name, network_type);
    outpath_node.push_str("_h3mapping.parquet");
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

    osm_loader_builder.target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path);
    match nodes_to_match_df {
        Some(df) => {osm_loader_builder.nodes_to_match_polars(df.clone());},
        _ => (),
    }
    match nodes_to_match_path {
        Some(path) => {osm_loader_builder.nodes_to_match_parquet(path);},
        None => ()
    }
    let osm_loader = osm_loader_builder.build()
        .expect("Parameter missing");
    let outpath_nodes = get_node_outpath(outpath, city_name, "pois");

    let nodes = osm_loader.load_graph();
    let output_file_nodes = File::create(outpath_nodes).unwrap();
    let node_writer = BufWriter::new(output_file_nodes);

    let parquet_writer = polars_io::parquet::write::ParquetWriter::new(node_writer);
    let mut df = struct_to_dataframe!(nodes, [osm_id, lat, long, nearest_osm_node, dist_to_nearest]).unwrap();
    parquet_writer.finish(&mut df).unwrap();
    df
}

pub fn _load_osm_walking(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<WalkingEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(WalkingEdgeFilter)
        .target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .reverse_edges(true)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "walking");
    let outpath_nodes = get_node_outpath(outpath, city_name, "walking");
    let outpath_mapping = get_mapping_outpath(outpath, city_name, "walking");

    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(
        &osm_loader,
        &outpath_edges,
        &outpath_nodes,
        &outpath_mapping,
    )
    .expect("Error in writing")
}
pub fn _load_osm_cycling(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    reverse_edges: &bool,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<BicycleEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(BicycleEdgeFilter)
        .target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .reverse_edges(*reverse_edges)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "cycling");
    let outpath_nodes = get_node_outpath(outpath, city_name, "cycling");
    let outpath_mapping = get_mapping_outpath(outpath, city_name, "cycling");
    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(
        &osm_loader,
        &outpath_edges,
        &outpath_nodes,
        &outpath_mapping,
    )
    .expect("Error in writing")
}
pub fn _load_osm_driving(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (DataFrame, DataFrame, DataFrame) {
    let bounding_box = Polygon::new(LineString::from(geometry_vec), vec![]);
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let osm_loader: Loader<CarEdgeFilter> = OsmLoaderBuilder::default()
        .edge_filter(CarEdgeFilter)
        .target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .pbf_path(pbf_path)
        .build()
        .expect("Parameter missing");
    let outpath_edges = get_edge_outpath(outpath, city_name, "driving");
    let outpath_nodes = get_node_outpath(outpath, city_name, "driving");
    let outpath_mapping = get_mapping_outpath(outpath, city_name, "driving");
    // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
    write_graph(
        &osm_loader,
        &outpath_edges,
        &outpath_nodes,
        &outpath_mapping,
    )
    .expect("Error in writing")
}

struct ClosestNode {
    node: crate::pbfextractor::pbf::Node,
    distance: Meters,
}
#[derive(Serialize, Deserialize)]
struct H3NodeMapping {
    node_id: u64,
    osm_node_id: u64,
    h3_cell_id: String,
}

fn write_graph<T: EdgeFilter>(
    l: &Loader<T>,
    outpath_edges: &str,
    outpath_nodes: &str,
    outpath_mapping: &str,
) -> Result<(DataFrame, DataFrame, DataFrame), io::Error> {
    let output_file_edges = File::create(outpath_edges).unwrap();
    let output_file_nodes = File::create(outpath_nodes).unwrap();
    let output_file_mapping = File::create(outpath_mapping).unwrap();
    let edge_writer = BufWriter::new(output_file_edges);
    let node_writer = BufWriter::new(output_file_nodes);
    let mapping_writer = BufWriter::new(output_file_mapping);

    let (nodes, edges) = l.load_graph();

    let mut parquet_writer = polars_io::parquet::write::ParquetWriter::new(edge_writer);
    let mut df_edges: polars::prelude::DataFrame = struct_to_dataframe!(edges, [source, source_osm, dest, dest_osm, length]).unwrap();
    parquet_writer.finish(&mut df_edges).unwrap();

    let mut h3_mapping = HashMap::new();
    for node in &nodes {
        let coord = LatLng::new(node.lat, node.long).expect("Coord should always be correct");
        let cell = coord.to_cell(Resolution::Eight);
        let center_coord = LatLng::from(cell);
        let center_as_node = crate::pbfextractor::pbf::Node::from_xy(center_coord.lat(), center_coord.lng());
        let dist: Meters = Distance_
            .calc(&node, &center_as_node, &l.proj_to_m)
            .expect("should be a valid distance");
        if h3_mapping.contains_key(&cell.to_string()) {
            let current_value: &ClosestNode = h3_mapping.get(&cell.to_string()).unwrap();
            if dist < current_value.distance {
                h3_mapping.insert(
                    cell.to_string(),
                    ClosestNode {
                        node: node.clone(),
                        distance: dist,
                    },
                );
            }
        } else {
            h3_mapping.insert(
                cell.to_string(),
                ClosestNode {
                    node: node.clone(),
                    distance: dist,
                },
            );
        }
    }
    parquet_writer = polars_io::parquet::write::ParquetWriter::new(node_writer);
    let mut df_nodes = struct_to_dataframe!(nodes, [osm_id, id, lat, long]).unwrap();
    parquet_writer.finish(&mut df_nodes).unwrap();
    parquet_writer = polars_io::parquet::write::ParquetWriter::new(mapping_writer);
    let mut df_mapping = struct_to_dataframe!(h3_mapping.iter().map(|(key, value)| H3NodeMapping{node_id: value.node.id, osm_node_id: value.node.osm_id, h3_cell_id: key.clone()}).collect::<Vec<H3NodeMapping>>(), [osm_node_id, h3_cell_id]).unwrap();
    parquet_writer.finish(&mut df_mapping).unwrap();
    Ok((df_nodes, df_edges, df_mapping))
}
