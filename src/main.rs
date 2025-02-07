use osmtools::extractor::load_osm_walking;

fn main() {
    let bounding_box = vec![
        (6.629850485818913, 50.7405089663172),
        (6.629850485818913, 51.1749294931249),
        (7.304073531148258, 51.1749294931249),
        (7.304073531148258, 50.7405089663172),
    ];

    _load_osm_walking("Koeln", bounding_box, "data", "data", true);
    // load_osm_cycling("Koeln", bounding_box, "data", "data", false);
}
