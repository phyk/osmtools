use super::sources::get_bbbike_source;
use std::fs::{create_dir_all, remove_file, File};
use std::io::{copy, Cursor};
use std::path::{Path, PathBuf};
use log::info;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn download_source(url: &String, filename: &String, target_dir: &String) -> Result<PathBuf> {
    let path = Path::new(target_dir);
    if !path.exists() {
        info!("Creating directories for path {target_dir}");
        match create_dir_all(path) {
            Ok(_) => (),
            Err(error) => panic!("Problem creating the target directory {error:?}"),
        }
    }
    let filepath_buf = path.join(Path::new(filename));
    let filepath = filepath_buf.as_path();
    if filepath.exists() {
        info!("Deleting file {filename} because it already existed at the specified location");
        match remove_file(filepath) {
            Ok(_) => (),
            Err(error) => panic!("Problem removing the existing pbf file: {error:?}"),
        }
    }
    info!("Downloading file");
    let response = reqwest::blocking::get(url)?;
    let mut file = File::create(filepath)?;
    let mut content = Cursor::new(response.bytes()?);
    info!("Writing contents to file");
    copy(&mut content, &mut file)?;
    Ok(filepath_buf)
}

pub fn download(source_name: &String, target_dir: &String) -> Result<PathBuf> {
    let (filename, url) = get_bbbike_source(source_name).expect("Not available at source BBBike");
    download_source(&url, &filename, target_dir)
}
