use super::pbf::{Latitude, LoaderBuildError, Longitude, OsmNodeId};
use geo::{Contains, Polygon};
use kiddo::ImmutableKdTree;
use kiddo::SquaredEuclidean;
use log::info;
use log::warn;
use osmpbfreader::{Node, OsmObj, OsmPbfReader};
use polars::prelude::DataFrame;
use polars_io::SerReader;
use std::iter::zip;
use proj::{Coord, Proj};
use serde::Serialize;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

pub struct PoiLoader {
    pbf_path: PathBuf,
    filter_geometry: Option<Polygon>,
    pub proj_to_m: Proj,
    kdtree: ImmutableKdTree<f64, 2>,
    nodes_to_match: Vec<super::pbf::Node>,
}

#[derive(Debug, Serialize)]
pub enum PoiType {
    Grocery,
    Education,
    Health,
    Banks,
    Parks,
    Sustenance,
    Shops,
}

#[derive(Debug, Serialize)]
pub struct Poi {
    pub osm_id: OsmNodeId,
    pub lat: Latitude,
    pub long: Longitude,
    pub nearest_osm_node: OsmNodeId,
    pub dist_to_nearest: f64,
    pub poi_type: PoiType,
}

impl Poi {
    fn new(
        osm_id: OsmNodeId,
        lat: Latitude,
        long: Longitude,
        nearest_osm_node: OsmNodeId,
        dist_to_nearest: f64,
        poi_type: PoiType,
    ) -> Poi {
        Poi {
            osm_id,
            lat,
            long,
            nearest_osm_node,
            dist_to_nearest,
            poi_type,
        }
    }
}

#[derive(Default)]
pub struct PoiLoaderBuilder {
    pbf_path: Option<PathBuf>,
    filter_geometry: Option<Polygon>,
    target_crs: Option<String>,
    nodes_to_match: Option<Vec<super::pbf::Node>>,
}

#[allow(dead_code)]
impl PoiLoaderBuilder {
    pub fn pbf_path<VALUE: Into<PathBuf>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.pbf_path = Some(value.into());
        new
    }
    pub fn pbf_path_from_str<VALUE: Into<String>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.pbf_path = Some(Path::new(&value.into()).to_path_buf());
        new
    }
    pub fn filter_geometry<VALUE: Into<Polygon>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.filter_geometry = Some(value.into());
        new
    }
    pub fn target_crs<VALUE: Into<String>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        new.target_crs = Some(value.into());
        new
    }
    pub fn nodes_to_match<VALUE: Into<Vec<super::pbf::Node>>>(
        &mut self,
        value: VALUE,
    ) -> &mut Self {
        let new = self;
        new.nodes_to_match = Some(value.into());
        new
    }
    pub fn nodes_to_match_parquet<VALUE: Into<String>>(&mut self, value: VALUE) -> &mut Self {
        let new = self;
        return match File::open(value.into()) {
            Ok(file) => {
                let node_reader = BufReader::new(file);
                let reader = polars_io::parquet::read::ParquetReader::new(node_reader).read_parallel(polars::prelude::ParallelStrategy::Auto);
                let df = reader.finish().unwrap();
                new.nodes_to_match_polars(df)
            }
            Err(error) => {
                warn!("{error}");
                warn!("The supplied File could not be opened for matching nodes");
                new
            }
        };
    }
    pub fn nodes_to_match_polars(&mut self, df: DataFrame) -> &mut Self {
        let new = self;
        new.nodes_to_match = Some(
            zip(df.column("osm_id").unwrap().u64().expect("wrong dtype on osm id").into_iter(), zip(df.column("lat").unwrap().f64().expect("Lat has wrong dtype").into_iter(), df.column("long").unwrap().f64().expect("Long has wrong dtype").into_iter())).map(
                |(osm_id, (lat, long))| super::pbf::Node::new(osm_id.unwrap(), lat.unwrap(), long.unwrap())
            ).collect()
        );
        new
    }
    pub fn build(&self) -> Result<PoiLoader, LoaderBuildError> {
        let target_crs = self
            .target_crs
            .as_ref()
            .expect("Requires CRS to be set for any calculation");
        let proj_to_m = Proj::new_known_crs("EPSG:4326", &target_crs, None)
            .expect("Error in creation of Projection");
        let nodes_to_match = match &self.nodes_to_match {
            Some(value) => value,
            None => panic!("Nodes are necessary for matching"),
        };
        let nodes_projected: Vec<[f64; 2]> = nodes_to_match
            .iter()
            .map(|n| proj_to_m.convert((n.x(), n.y())).unwrap().into())
            .collect();
        let kdtree = ImmutableKdTree::new_from_slice(&nodes_projected);

        Ok(PoiLoader {
            pbf_path: match self.pbf_path {
                Some(ref value) => Clone::clone(value),
                None => return Err(LoaderBuildError::new("pbf_path".into())),
            },
            filter_geometry: Clone::clone(&self.filter_geometry),
            proj_to_m,
            nodes_to_match: nodes_to_match.to_owned(),
            kdtree,
        })
    }
}

impl PoiLoader {
    /// Loads the graph from a pbf file.
    pub fn load_graph(&self) -> Vec<Poi> {
        info!(
            "Extracting data out of: {}",
            self.pbf_path
                .to_str()
                .expect("Path could not be converted to string")
        );
        let fs = File::open(self.pbf_path.as_path()).unwrap();
        let mut reader = OsmPbfReader::new(fs);

        let mut skipped_nodes = 0;

        let nodes: Vec<Poi> = reader
            .par_iter()
            .filter_map(|obj| {
                if let Ok(OsmObj::Node(n)) = obj {
                    let lat = f64::from(n.decimicro_lat) / 10_000_000.0;
                    let lng = f64::from(n.decimicro_lon) / 10_000_000.0;
                    let point = geo::Point::new(lng, lat);
                    let point_convert = self.proj_to_m.convert(point).unwrap();
                    let nearest_node = self
                        .kdtree
                        .nearest_one::<SquaredEuclidean>(&[point_convert.x(), point_convert.y()]);
                    let osm_nearest_node: &super::pbf::Node = self
                        .nodes_to_match
                        .get::<usize>(nearest_node.item as usize)
                        .expect("Impossible, all nodes have to exist");
                    if self
                        .filter_geometry
                        .as_ref()
                        .is_some_and(|f| !f.contains(&point))
                    {
                        skipped_nodes += 1;
                        None
                    } else {
                        match identify_type(&n) {
                            Some(v) => Some(Poi::new(
                                n.id.0.try_into().unwrap(),
                                lat,
                                lng,
                                osm_nearest_node.osm_id,
                                nearest_node.distance.sqrt(),
                                v,
                            )),
                            None => None,
                        }
                    }
                } else {
                    None
                }
            })
            .collect();

        info!("Collected {} nodes", nodes.len());
        info!("Calculating Metrics");

        nodes
    }
}

const PARKS_ATTRIBUTES: &[(&str, &str)] = &[("leisure", "park"), ("leisure", "dog park")];
const GROCERY_ATTRIBUTES: &[(&str, &str)] = &[
    ("shop", "alcohol"),
    ("shop", "bakery"),
    ("shop", "beverages"),
    ("shop", "brewing supplies"),
    ("shop", "butcher"),
    ("shop", "cheese"),
    ("shop", "chocolate"),
    ("shop", "coffee"),
    ("shop", "confectionery"),
    ("shop", "convenience"),
    ("shop", "deli"),
    ("shop", "dairy"),
    ("shop", "farm"),
    ("shop", "frozen food"),
    ("shop", "greengrocer"),
    ("shop", "health food"),
    ("shop", "ice-cream"),
    ("shop", "pasta"),
    ("shop", "pastry"),
    ("shop", "seafood"),
    ("shop", "spices"),
    ("shop", "tea"),
    ("shop", "water"),
    ("shop", "supermarket"),
    ("shop", "department store"),
    ("shop", "general"),
    ("shop", "kiosk"),
    ("shop", "mall"),
];
const EDUCATION_ATTRIBUTES: &[(&str, &str)] = &[
    ("amenity", "college"),
    ("amenity", "driving school"),
    ("amenity", "kindergarten"),
    ("amenity", "language school"),
    ("amenity", "music school"),
    ("amenity", "school"),
    ("amenity", "university"),
];
const HEALTH_ATTRIBUTES: &[(&str, &str)] = &[
    ("amenity", "clinic"),
    ("amenity", "dentist"),
    ("amenity", "doctors"),
    ("amenity", "hospital"),
    ("amenity", "nursing home"),
    ("amenity", "pharmacy"),
    ("amenity", "social facility"),
];
const BANKS_ATTRIBUTES: &[(&str, &str)] = &[
    ("amenity", "atm"),
    ("amenity", "bank"),
    ("amenity", "bureau de change"),
    ("amenity", "post office"),
];
const SUSTENANCE_ATTRIBUTES: &[(&str, &str)] = &[
    ("amenity", "restaurant"),
    ("amenity", "pub"),
    ("amenity", "bar"),
    ("amenity", "cafe"),
    ("amenity", "fast-food"),
    ("amenity", "food court"),
    ("amenity", "ice-cream"),
    ("amenity", "biergarten"),
];
const SHOPS_QUERY: &[(&str, &str)] = &[
    ("shop", "department store"),
    ("shop", "general"),
    ("shop", "kiosk"),
    ("shop", "mall"),
    ("shop", "wholesale"),
    ("shop", "baby goods"),
    ("shop", "bag"),
    ("shop", "boutique"),
    ("shop", "clothes"),
    ("shop", "fabric"),
    ("shop", "fashion accessories"),
    ("shop", "jewelry"),
    ("shop", "leather"),
    ("shop", "watches"),
    ("shop", "wool"),
    ("shop", "charity"),
    ("shop", "secondhand"),
    ("shop", "variety store"),
    ("shop", "beauty"),
    ("shop", "chemist"),
    ("shop", "cosmetics"),
    ("shop", "erotic"),
    ("shop", "hairdresser"),
    ("shop", "hairdresser supply"),
    ("shop", "hearing aids"),
    ("shop", "herbalist"),
    ("shop", "massage"),
    ("shop", "medical supply"),
    ("shop", "nutrition supplements"),
    ("shop", "optician"),
    ("shop", "perfumery"),
    ("shop", "tattoo"),
    ("shop", "agrarian"),
    ("shop", "appliance"),
    ("shop", "bathroom furnishing"),
    ("shop", "do-it-yourself"),
    ("shop", "electrical"),
    ("shop", "energy"),
    ("shop", "ﬁreplace"),
    ("shop", "ﬂorist"),
    ("shop", "garden centre"),
    ("shop", "garden furniture"),
    // ("shop", "gas"),
    ("amenity", "fuel"),
    ("shop", "glaziery"),
    ("shop", "groundskeeping"),
    ("shop", "hardware"),
    ("shop", "houseware"),
    ("shop", "locksmith"),
    ("shop", "paint"),
    ("shop", "security"),
    ("shop", "trade"),
    ("shop", "antiques"),
    ("shop", "bed"),
    ("shop", "candles"),
    ("shop", "carpet"),
    ("shop", "curtain"),
    ("shop", "doors"),
    ("shop", "ﬂooring"),
    ("shop", "furniture"),
    ("shop", "household linen"),
    ("shop", "interior decoration"),
    ("shop", "kitchen"),
    ("shop", "lighting"),
    ("shop", "tiles"),
    ("shop", "window blind"),
    ("shop", "computer"),
    ("shop", "electronics"),
    ("shop", "hiﬁ"),
    ("shop", "mobile phone"),
    ("shop", "radio-technics"),
    ("shop", "vacuum cleaner"),
    ("shop", "bicycle"),
    ("shop", "boat"),
    ("shop", "car"),
    ("shop", "car"),
    ("shop", "repair"),
    ("shop", "car parts"),
    ("shop", "caravan"),
    ("shop", "fuel"),
    ("shop", "ﬁshing"),
    ("shop", "golf"),
    ("shop", "hunting"),
    ("shop", "jet ski"),
    ("shop", "military surplus"),
    ("shop", "motorcycle"),
    ("shop", "outdoor"),
    ("shop", "scuba diving"),
    ("shop", "ski"),
    ("shop", "snowmobile"),
    ("shop", "swimming pool"),
    ("shop", "trailer"),
    ("shop", "tyres"),
    ("shop", "art"),
    ("shop", "collector"),
    ("shop", "craft"),
    ("shop", "frame"),
    ("shop", "games"),
    ("shop", "model"),
    ("shop", "music"),
    ("shop", "musical instrument"),
    ("shop", "photo"),
    ("shop", "camera"),
    ("shop", "trophy"),
    ("shop", "video"),
    ("shop", "videogames"),
    ("shop", "anime"),
    ("shop", "books"),
    ("shop", "gift"),
    ("shop", "lottery"),
    ("shop", "newsagent"),
    ("shop", "stationery"),
    ("shop", "ticket"),
    ("shop", "bookmaker"),
    ("shop", "cannabis"),
    ("shop", "copy node"),
    ("shop", "drycleaning"),
    ("shop", "e-cigarette"),
    ("shop", "funeral directors"),
    ("shop", "laundry"),
    ("shop", "moneylender"),
    ("shop", "party"),
    ("shop", "pawnbroker"),
    ("shop", "pet"),
    ("shop", "pet"),
    ("shop", "grooming"),
    ("shop", "pest control"),
    ("shop", "pyrotechnics"),
    ("shop", "religion"),
    ("shop", "storage rental"),
    ("shop", "tobacco"),
    ("shop", "toys"),
    ("shop", "travel agency"),
    ("shop", "vacant"),
    ("shop", "weapons"),
    ("shop", "outpost"),
];

fn identify_type(n: &Node) -> Option<PoiType> {
    let is_park = PARKS_ATTRIBUTES.iter().any(|(k, v)| n.tags.contains(k, v));
    if is_park {
        return Some(PoiType::Parks);
    }
    let is_bank = BANKS_ATTRIBUTES.iter().any(|(k, v)| n.tags.contains(k, v));
    if is_bank {
        return Some(PoiType::Banks);
    }
    let is_health = HEALTH_ATTRIBUTES.iter().any(|(k, v)| n.tags.contains(k, v));
    if is_health {
        return Some(PoiType::Health);
    }
    let is_education = EDUCATION_ATTRIBUTES
        .iter()
        .any(|(k, v)| n.tags.contains(k, v));
    if is_education {
        return Some(PoiType::Education);
    }
    let is_sustenance = SUSTENANCE_ATTRIBUTES
        .iter()
        .any(|(k, v)| n.tags.contains(k, v));
    if is_sustenance {
        return Some(PoiType::Sustenance);
    }
    let is_grocery = GROCERY_ATTRIBUTES
        .iter()
        .any(|(k, v)| n.tags.contains(k, v));
    if is_grocery {
        return Some(PoiType::Grocery);
    }
    let is_shop = SHOPS_QUERY.iter().any(|(k, v)| n.tags.contains(k, v));
    if is_shop {
        return Some(PoiType::Shops);
    }
    None
}
