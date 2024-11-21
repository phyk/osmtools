use reqwest::header;

fn main() {
    osmtools::download::download(
        &"".to_string(),
        &"file.osm.pbf".to_string(),
        &"data".to_string(),
    );
}
