
use std::{fs, fs::OpenOptions};
use std::io::{BufReader, BufRead};
use std::env;

/// This takes two arguments, filename(in raw_data) and a number of lines
/// It then removes the number of lines from the front of the file provided
/// For the Apple export I have found this to be 212 lines
fn main() {

    println!("Test");

    let mut args = env::args();
    args.next();

    // Read the file
    let filename: &str = &(String::from("./../data/raw_data/") + &args.next().unwrap());
    let num_lines = args.next().unwrap().parse().unwrap();
    // let filename: &str = "./../data/raw_data/test.txt";
    // let filename: &str = "data/raw_data/export-partial-2022-11-27.xml";
    let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(filename)
                    .expect("file doesn't exist");

    let lines = BufReader::new(file).lines().skip(num_lines)
                    .map(|x| x.unwrap())
                    .collect::<Vec<String>>().join("\n");
    
    fs::write(filename, lines).expect("Can't write.");
}
