use geo::polygon;
use osmtools::pbfextractor::metrics::{CarEdgeFilter, Distance_, EdgeFilter};
use osmtools::pbfextractor::pbf::{Loader, NodeMetrics, OsmLoaderBuilder};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::rc::Rc;
use std::time::SystemTime;

fn main() {
    let pbf_path =
        osmtools::download::download(&"Koeln".into(), &"data".into()).expect("Not downloaded");
    let outpath = "data/edges.csv";
    let dist = Rc::new(Distance_);
    let node_metrics: NodeMetrics = vec![dist];
    let bounding_box = polygon![(x: 6.629850485818913, y: 50.7405089663172), (x: 6.629850485818913, y: 51.1749294931249), (x: 7.304073531148258, y: 51.1749294931249), (x: 7.304073531148258, y: 50.7405089663172)];

    // TODO:
    // use largest connected component only
    // calculate car speed max for all car edges
    let osm_loader: Loader<CarEdgeFilter> = OsmLoaderBuilder::default()
        .pbf_path(pbf_path)
        .edge_filter(CarEdgeFilter)
        .node_metrics(node_metrics)
        .target_crs("EPSG:4839")
        .filter_geometry(bounding_box)
        .build()
        .expect("What is missing?");

    // change output format to a more efficient read/write format or a better structured format (or both)
    // try out serde for writing the graph
    let output_file = File::create(outpath).unwrap();
    let graph = BufWriter::new(output_file);
    if false {
        let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
        write_graph(&osm_loader, graph).expect("Error in writing");
    } else {
        write_graph(&osm_loader, graph).expect("Error in writing");
    }
}

fn write_graph<T: EdgeFilter, W: Write>(l: &Loader<T>, graph: W) -> Result<(), io::Error> {
    let (nodes, edges) = l.load_graph();
    let mut wtr = csv::Writer::from_writer(graph);

    for edge in edges {
        print!("{}", edge.source);
        wtr.serialize(edge)?;
    }
    wtr.flush()?;
    Ok(())
}
