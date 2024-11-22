use osmtools::pbfextractor::pbf;
use std::fs::File;
use std::io::BufWriter;

fn main() {
    let pbf_path =
        osmtools::download::download(&"Koeln".into(), &"data".into()).expect("Not downloaded");
    let l = pbf::Loader::new(
        pbf_path,
        CarEdgeFilter,
        tag_metrics,
        node_metrics,
        cost_metrics,
        internal_only_metrics,
    );

    let output_file = File::create(output).unwrap();
    let graph = BufWriter::new(output_file);
    if zip {
        let graph = flate2::write::GzEncoder::new(graph, flate2::Compression::best());
        write_graph(&l, graph);
    } else {
        write_graph(&l, graph);
    }
}

fn write_graph<T: EdgeFilter, W: Write>(l: &Loader<T>, mut graph: W) {
    let (nodes, edges) = l.load_graph();

    writeln!(&mut graph, "# Build by: pbfextractor").unwrap();
    writeln!(&mut graph, "# Build on: {:?}", SystemTime::now()).unwrap();
    write!(&mut graph, "# metrics: ").unwrap();

    for metric in l.metrics_indices.keys() {
        if l.internal_metrics.contains(metric) {
            continue;
        }
        write!(&mut graph, "{}, ", metric).unwrap();
    }

    write!(&mut graph, "\n\n").unwrap();

    writeln!(&mut graph, "{}", l.metric_count()).unwrap();
    writeln!(&mut graph, "{}", nodes.len()).unwrap();
    writeln!(&mut graph, "{}", edges.len()).unwrap();

    for (i, node) in nodes.iter().enumerate() {
        writeln!(
            &mut graph,
            "{} {} {} {} {} 0",
            i, node.osm_id, node.lat, node.long, node.height,
        )
        .unwrap();
    }
    for edge in &edges {
        write!(&mut graph, "{} {} ", edge.source, edge.dest).unwrap();
        for cost in &edge.costs(&l.metrics_indices, &l.internal_metrics) {
            write!(&mut graph, "{} ", cost.round()).unwrap();
        }
        writeln!(&mut graph, "-1 -1").unwrap();
    }
    graph.flush().unwrap();
}
