use geo::polygon;
use osmtools::pbfextractor::metrics::{CarEdgeFilter, EdgeFilter};
use osmtools::pbfextractor::pbf::{Loader, OsmLoaderBuilder};
use std::fs::File;
use std::io::{self, BufWriter};

fn main() {
    let pbf_path =
        osmtools::download::download(&"Koeln".into(), &"data".into()).expect("Not downloaded");
    let outpath_edges = "data/edges.csv";
    let outpath_nodes = "data/nodes.csv";
    let bounding_box = polygon![(x: 6.629850485818913, y: 50.7405089663172), (x: 6.629850485818913, y: 51.1749294931249), (x: 7.304073531148258, y: 51.1749294931249), (x: 7.304073531148258, y: 50.7405089663172)];

    // TODO:
    // use largest connected component only
    // calculate car speed max for all car edges
    // add BikeEdgeFilter and WalkingEdgeFilter
    let osm_loader: Loader<CarEdgeFilter> = OsmLoaderBuilder::default()
        .pbf_path(pbf_path)
        .edge_filter(CarEdgeFilter)
        .target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .build()
        .expect("What is missing?");

        // let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());

    write_graph(&osm_loader, outpath_edges, outpath_nodes).expect("Error in writing");
}

fn write_graph<T: EdgeFilter>(l: &Loader<T>, outpath_edges: &str, outpath_nodes: &str) -> Result<(), io::Error> {
    let output_file_edges = File::create(outpath_edges).unwrap();
    let output_file_nodes = File::create(outpath_nodes).unwrap();
    let edge_writer = BufWriter::new(output_file_edges);
    let node_writer = BufWriter::new(output_file_nodes);

    let (nodes, edges) = l.load_graph();

    let mut wtr = csv::Writer::from_writer(edge_writer);
    for edge in edges {
        wtr.serialize(edge)?;
    }
    wtr.flush()?;
    wtr = csv::Writer::from_writer(node_writer);
    for node in nodes {
        wtr.serialize(node)?;
    }
    wtr.flush()?;
    Ok(())
}
