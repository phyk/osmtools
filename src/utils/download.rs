use std::fs::{create_dir_all, remove_file, File};
use std::io::{copy, Cursor};
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn download(url: &String, filename: &String, target_dir: &String) -> Result<()> {
    let path = Path::new(target_dir);
    if !path.exists() {
        match create_dir_all(path) {
            Ok(_) => (),
            Err(error) => panic!("Problem creating the target directory {error:?}"),
        }
    }
    let filepath = path.join(Path::new(filename));
    if filepath.exists() {
        match remove_file(filepath) {
            Ok(_) => (),
            Err(error) => panic!("Problem removing the existing pbf file: {error:?}"),
        }
    }
    let response = reqwest::blocking::get(url)?;
    let mut file = File::create(path)?;
    let mut content = Cursor::new(response.bytes()?);
    copy(&mut content, &mut file)?;
    Ok(())
}
