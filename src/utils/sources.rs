use core::fmt;
use std::error::Error;

#[derive(Debug, PartialEq)]
pub struct SourceNotFoundError {
    source_name: String,
}

impl Error for SourceNotFoundError {}
impl SourceNotFoundError {
    fn new(source_name: String) -> Self {
        Self { source_name }
    }
}

impl fmt::Display for SourceNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The required source {} was not found.",
            &self.source_name
        )
    }
}

const CITIES: [&str; 235] = [
    "Aachen",
    "Aarhus",
    "Adelaide",
    "Albuquerque",
    "Alexandria",
    "Amsterdam",
    "Antwerpen",
    "Arnhem",
    "Auckland",
    "Augsburg",
    "Austin",
    "Baghdad",
    "Baku",
    "Balaton",
    "Bamberg",
    "Bangkok",
    "Barcelona",
    "Basel",
    "Beijing",
    "Beirut",
    "Berkeley",
    "Berlin",
    "Bern",
    "Bielefeld",
    "Birmingham",
    "Bochum",
    "Bogota",
    "Bombay",
    "Bonn",
    "Bordeaux",
    "Boulder",
    "BrandenburgHavel",
    "Braunschweig",
    "Bremen",
    "Bremerhaven",
    "Brisbane",
    "Bristol",
    "Brno",
    "Bruegge",
    "Bruessel",
    "Budapest",
    "BuenosAires",
    "Cairo",
    "Calgary",
    "Cambridge",
    "CambridgeMa",
    "Canberra",
    "CapeTown",
    "Chemnitz",
    "Chicago",
    "ClermontFerrand",
    "Colmar",
    "Copenhagen",
    "Cork",
    "Corsica",
    "Corvallis",
    "Cottbus",
    "Cracow",
    "CraterLake",
    "Curitiba",
    "Cusco",
    "Dallas",
    "Darmstadt",
    "Davis",
    "DenHaag",
    "Denver",
    "Dessau",
    "Dortmund",
    "Dresden",
    "Dublin",
    "Duesseldorf",
    "Duisburg",
    "Edinburgh",
    "Eindhoven",
    "Emden",
    "Erfurt",
    "Erlangen",
    "Eugene",
    "Flensburg",
    "FortCollins",
    "Frankfurt",
    "FrankfurtOder",
    "Freiburg",
    "Gdansk",
    "Genf",
    "Gent",
    "Gera",
    "Glasgow",
    "Gliwice",
    "Goerlitz",
    "Goeteborg",
    "Goettingen",
    "Graz",
    "Groningen",
    "Halifax",
    "Halle",
    "Hamburg",
    "Hamm",
    "Hannover",
    "Heilbronn",
    "Helsinki",
    "Hertogenbosch",
    "Huntsville",
    "Innsbruck",
    "Istanbul",
    "Jena",
    "Jerusalem",
    "Johannesburg",
    "Kaiserslautern",
    "Karlsruhe",
    "Kassel",
    "Katowice",
    "Kaunas",
    "Kiel",
    "Kiew",
    "Koblenz",
    "Koeln",
    "Konstanz",
    "LaPaz",
    "LaPlata",
    "LakeGarda",
    "Lausanne",
    "Leeds",
    "Leipzig",
    "Lima",
    "Linz",
    "Lisbon",
    "Liverpool",
    "Ljubljana",
    "Lodz",
    "London",
    "Luebeck",
    "Luxemburg",
    "Lyon",
    "Maastricht",
    "Madison",
    "Madrid",
    "Magdeburg",
    "Mainz",
    "Malmoe",
    "Manchester",
    "Mannheim",
    "Marseille",
    "Melbourne",
    "Memphis",
    "MexicoCity",
    "Miami",
    "Moenchengladbach",
    "Montevideo",
    "Montpellier",
    "Montreal",
    "Moscow",
    "Muenchen",
    "Muenster",
    "NewDelhi",
    "NewOrleans",
    "NewYorkCity",
    "Nuernberg",
    "Oldenburg",
    "Oranienburg",
    "Orlando",
    "Oslo",
    "Osnabrueck",
    "Ostrava",
    "Ottawa",
    "Paderborn",
    "Palma",
    "PaloAlto",
    "Paris",
    "Perth",
    "Philadelphia",
    "PhnomPenh",
    "Portland",
    "PortlandME",
    "Porto",
    "PortoAlegre",
    "Potsdam",
    "Poznan",
    "Prag",
    "Providence",
    "Regensburg",
    "Riga",
    "RiodeJaneiro",
    "Rostock",
    "Rotterdam",
    "Ruegen",
    "Saarbruecken",
    "Sacramento",
    "Saigon",
    "Salzburg",
    "SanFrancisco",
    "SanJose",
    "SanktPetersburg",
    "SantaBarbara",
    "SantaCruz",
    "Santiago",
    "Sarajewo",
    "Schwerin",
    "Seattle",
    "Seoul",
    "Sheffield",
    "Singapore",
    "Sofia",
    "Stockholm",
    "Stockton",
    "Strassburg",
    "Stuttgart",
    "Sucre",
    "Sydney",
    "Szczecin",
    "Tallinn",
    "Tehran",
    "Tilburg",
    "Tokyo",
    "Toronto",
    "Toulouse",
    "Trondheim",
    "Tucson",
    "Turin",
    "UlanBator",
    "Ulm",
    "Usedom",
    "Utrecht",
    "Vancouver",
    "Victoria",
    "WarenMueritz",
    "Warsaw",
    "WashingtonDC",
    "Waterloo",
    "Wien",
    "Wroclaw",
    "Wuerzburg",
    "Wuppertal",
    "Zagreb",
    "Zuerich",
];

#[allow(dead_code)]
pub fn get_bbbike_source(city_name: &String) -> Result<(String, String), SourceNotFoundError> {
    let base_url = "https://download.bbbike.org/osm/bbbike";
    let suffix = ".osm.pbf";
    let mut filename = String::new();
    let mut url = String::new();
    let mut found = false;
    for city in CITIES {
        let city_lower = city.to_lowercase();
        if city_lower == city_name.to_lowercase() {
            found = true;
            match city_lower.as_str() {
                "newyorkcity" => {
                    filename = city_lower + suffix;
                    url = format!("{base_url}/NewYork/NewYork{suffix}");
                }
                _ => {
                    filename = city_lower + suffix;
                    url = format!("{base_url}/{city}/{city}{suffix}");
                }
            };
        }
    }
    if found {
        Ok((filename, url))
    } else {
        Err(SourceNotFoundError::new(city_name.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_existing_city() -> Result<(), SourceNotFoundError> {
        let existing_city = "Zuerich";
        assert_eq!(
            get_bbbike_source(&existing_city.into())?,
            (
                "zuerich.osm.pbf".into(),
                "https://download.bbbike.org/osm/bbbike/Zuerich/Zuerich.osm.pbf".into()
            )
        );
        Ok(())
    }

    #[test]
    fn test_for_nonexistant_city() {
        let nonexistant_city = "Hogwarts";
        let result = get_bbbike_source(&nonexistant_city.into()).err();
        assert_eq!(
            result,
            Some(SourceNotFoundError::new(nonexistant_city.into()))
        );
    }

    #[test]
    fn test_for_newyorkcity() -> Result<(), SourceNotFoundError> {
        let existing_city = "NewYorkCity";
        assert_eq!(
            get_bbbike_source(&existing_city.into())?,
            (
                "newyorkcity.osm.pbf".into(),
                "https://download.bbbike.org/osm/bbbike/NewYork/NewYork.osm.pbf".into()
            )
        );
        Ok(())
    }
}
