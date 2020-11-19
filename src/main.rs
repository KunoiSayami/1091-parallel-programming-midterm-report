/*
 ** Copyright (C) 2020 KunoiSayami
 **
 ** This file is part of 1091-parallel-programming-midterm-report and is released under
 ** the AGPL v3 License: https://www.gnu.org/licenses/agpl-3.0.txt
 **
 ** This program is free software: you can redistribute it and/or modify
 ** it under the terms of the GNU Affero General Public License as published by
 ** the Free Software Foundation, either version 3 of the License, or
 ** any later version.
 **
 ** This program is distributed in the hope that it will be useful,
 ** but WITHOUT ANY WARRANTY; without even the implied warranty of
 ** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 ** GNU Affero General Public License for more details.
 **
 ** You should have received a copy of the GNU Affero General Public License
 ** along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
use std::fs::{File, OpenOptions, read};
use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};


#[derive(Debug)]
struct CountableFile {
    count: usize,
    file: File,
}


// This is the main thread
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let nthreads = num_cpus::get();
    match args.len() {
        2 => sub_task(&args[1], nthreads),
        3 => sub_task(&args[1], args[2].parse::<usize>().unwrap()),
        _ => println!("Please use <exec_name> file_to_compress [thread_nums]")
    }
}

fn wrapper(lock: Arc<Mutex<CountableFile>>, thread_id: usize, buffer: Option<&mut [u8]>, write_buffer: Option<&[u8]>) -> usize {
    loop {
        {
            let mut num = lock.lock().unwrap();
            println!("num {}", num.count);
            if num.count % (thread_id + 1) != 0 {
                let size = match buffer {
                    Some(buf) => num.file.read(buf).expect("fail"),
                    _ => match write_buffer {
                        Some(buf) => num.file.write(buf).unwrap(),
                        _ => panic!("Only one params can be none")
                    }
                };
                num.count += 1;
                return size;
            }
        }

        sleep(Duration::from_millis(5));
    }
}

fn sub_task(path_to_ref: &str, thread_nums: usize) {
    println!("Use: {} thread(s)", thread_nums);
    let path_to = String::from(path_to_ref);
    let path_to_out = format!("{}.gz", path_to_ref);
    // Make a vector to hold the children which are spawned.
    let mut children = vec![];
    let input_file:  File = match File::open(path_to.clone()) {
        Ok(f) => f,
        Err(error) => {
            panic!("Open {:?} error {:?}", path_to, error);
        }
    };

    let output_file:  File = match OpenOptions::new()
        .write(true)
        .open(path_to_out.clone()) {
        Ok(f) => f,
        Err(error) => {
            panic!("Open {:?} error {:?}", path_to_out, error);
        }
    };
    let read_lock = Arc::new(Mutex::new(
        CountableFile {count:1, file: input_file }));
    let write_lock = Arc::new(Mutex::new(
        CountableFile {count:1, file: output_file }));
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for thread_id in 0..thread_nums {
        let read_lock = Arc::clone(&read_lock);
        let write_lock = Arc::clone(&write_lock);
        // Spin up another thread
        children.push(thread::spawn(move || {
            let mut buffer = [0; 1024];
            let mut read_size: usize;
            //let mut gz_buffer = [0; 1024];
            loop {
                read_size = wrapper(read_lock.clone(), thread_id, Some(&mut buffer), None);
                println!("thread: {} read_size: {}", thread_id, read_size);
                encoder.write_all(&buffer[..read_size]).unwrap();
                wrapper(write_lock.clone(), thread_id, None, Some(encoder.finish().unwrap().as_slice()));
                if read_size < 1024 {
                    break;
                }
            }
        }));
    }

    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }
}
