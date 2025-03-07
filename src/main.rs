use osmtools::extractor::_load_osm_pois;

fn main() {
    let bounding_box = vec![
        (6.629850485818913, 50.7405089663172),
        (6.629850485818913, 51.1749294931249),
        (7.304073531148258, 51.1749294931249),
        (7.304073531148258, 50.7405089663172),
    ];

    _load_osm_pois("Koeln", bounding_box, "data", "data/koeln_walking_nodes.csv", "data", false);
    // _load_osm_walking("Koeln", bounding_box, "data", "data", true);
    // load_osm_cycling("Koeln", bounding_box, "data", "data", false);
}
