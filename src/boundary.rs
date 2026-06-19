use std::collections::BTreeMap;
use std::fs::File;

use geo::{Contains, LineString, Point, Polygon};
use osmpbfreader::{NodeId, OsmId, OsmObj, OsmPbfReader, Relation};

use crate::extractor::check_pbf_archives;

type Ring = Vec<(f64, f64)>;

fn is_target_relation(rel: &Relation, name_filter: &str, admin_level: &str) -> bool {
    rel.tags.contains("boundary", "administrative")
        && rel.tags.contains("admin_level", admin_level)
        && rel.tags.contains("name", name_filter)
}

/// Chains way node-id sequences sharing endpoints into closed rings.
///
/// Administrative boundary relations split their outer/inner rings across
/// many member ways; this reassembles them the same way osm2geojson does
/// server-side. A way that can't be closed (bad data) is kept as-is rather
/// than looping forever.
fn assemble_rings(mut segments: Vec<Vec<NodeId>>) -> Vec<Vec<NodeId>> {
    let mut rings = Vec::new();
    while let Some(mut ring) = segments.pop() {
        while ring.first() != ring.last() {
            let last = *ring.last().expect("ring is never empty");
            let Some(pos) = segments
                .iter()
                .position(|seg| seg.first() == Some(&last) || seg.last() == Some(&last))
            else {
                break;
            };
            let mut next = segments.remove(pos);
            if next.first() == Some(&last) {
                ring.extend(next.drain(1..));
            } else {
                next.reverse();
                ring.extend(next.drain(1..));
            }
        }
        rings.push(ring);
    }
    rings
}

fn ring_coords(objs: &BTreeMap<OsmId, OsmObj>, ring: &[NodeId]) -> Ring {
    ring.iter()
        .map(|node_id| {
            let node = objs
                .get(&OsmId::Node(*node_id))
                .and_then(OsmObj::node)
                .expect("Node referenced by boundary way not found in pbf");
            (node.lon(), node.lat())
        })
        .collect()
}

/// Extracts the administrative boundary of `name_filter` at `admin_level` from
/// the local PBF, returning (shell, holes) pairs as plain coordinate rings --
/// the same shape `shapely.geometry.MultiPolygon` expects -- so the caller
/// never needs a live Overpass API query for this.
pub fn _load_osm_boundary(
    city_name: &str,
    name_filter: &str,
    admin_level: &str,
    archive_path: &str,
    download: bool,
) -> Vec<(Ring, Vec<Ring>)> {
    let pbf_path = check_pbf_archives(city_name, archive_path, download)
        .expect("Download failed or Path not existing");
    let file = File::open(&pbf_path).expect("Could not open pbf file");
    let mut pbf = OsmPbfReader::new(file);

    let objs = pbf
        .get_objs_and_deps(|obj| match obj {
            OsmObj::Relation(rel) => is_target_relation(rel, name_filter, admin_level),
            _ => false,
        })
        .expect("Failed to read pbf file");

    let mut outer_segments: Vec<Vec<NodeId>> = Vec::new();
    let mut inner_segments: Vec<Vec<NodeId>> = Vec::new();
    for rel in objs
        .values()
        .filter_map(OsmObj::relation)
        .filter(|rel| is_target_relation(rel, name_filter, admin_level))
    {
        for member in &rel.refs {
            let OsmId::Way(way_id) = member.member else {
                continue;
            };
            let Some(way) = objs.get(&OsmId::Way(way_id)).and_then(OsmObj::way) else {
                continue;
            };
            match member.role.as_str() {
                "outer" => outer_segments.push(way.nodes.clone()),
                "inner" => inner_segments.push(way.nodes.clone()),
                _ => {}
            }
        }
    }

    assert!(
        !outer_segments.is_empty(),
        "No administrative boundary relation found for name={name_filter}, admin_level={admin_level} in {}",
        pbf_path.display()
    );

    let outer_rings: Vec<Ring> = assemble_rings(outer_segments)
        .iter()
        .map(|ring| ring_coords(&objs, ring))
        .collect();
    let inner_rings: Vec<Ring> = assemble_rings(inner_segments)
        .iter()
        .map(|ring| ring_coords(&objs, ring))
        .collect();

    let outer_polygons: Vec<Polygon> = outer_rings
        .iter()
        .map(|ring| Polygon::new(LineString::from(ring.clone()), vec![]))
        .collect();

    let mut holes_by_outer: Vec<Vec<Ring>> = vec![Vec::new(); outer_rings.len()];
    for inner in inner_rings {
        let Some(&(lon, lat)) = inner.first() else {
            continue;
        };
        let point = Point::new(lon, lat);
        if let Some(idx) = outer_polygons.iter().position(|p| p.contains(&point)) {
            holes_by_outer[idx].push(inner);
        }
    }

    outer_rings.into_iter().zip(holes_by_outer).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_test_osm_boundary() {
        let polygons = _load_osm_boundary("Bruegge", "Brugge", "9", "data", false);
        assert!(!polygons.is_empty());
        let (shell, _holes) = &polygons[0];
        assert!(shell.len() > 3);
        assert_eq!(shell.first(), shell.last());
    }
}
