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
use std::fs::{File, OpenOptions};
use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write, ErrorKind};

const BUFFER_SIZE: usize = 1024 * 256;

#[derive(Debug)]
struct CountableFile {
    count: usize,
    file: File,
    finish: bool
}

#[derive(Debug)]
struct CountableGzip {
    count: usize,
    encoder: GzEncoder<Vec<u8>>,
    finish: bool
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

fn wrapper(lock: &Arc<Mutex<CountableFile>>, thread_id: usize, buffer: &mut [u8]) -> usize {
    loop {
        {
            let mut num = lock.lock().unwrap();
            //println!("num {}", num.count);
            if num.finish {
                return 0;
            }
            if num.count % (thread_id + 1) == 0 {
                let size = num.file.read(buffer).expect("fail");
                num.count += 1;
                if size < BUFFER_SIZE {
                    num.finish = true;
                }
                return size;
            }
            //println!("{}", num.count)
        }
        sleep(Duration::from_millis(5));
    }
}

fn gzip_wrapper(lock: &Arc<Mutex<CountableGzip>>, thread_id: usize, buffer: &[u8]) {
    loop {
        {
            let mut num = lock.lock().unwrap();
            //println!("num {}", num.count);
            if num.finish {
                break
            }
            if num.count % (thread_id + 1) == 0 {
                num.encoder.write_all(buffer).unwrap();
                num.count += 1;
                break;
            }
            //println!("{}", num.count)
        }
        sleep(Duration::from_millis(5));
    }
}

fn sub_task(path_to_ref: &str, thread_nums: usize) {
    println!("Use: {} thread(s)", thread_nums);
    let path_to = String::from(path_to_ref);
    let path_to_out = format!("{}.gz", path_to_ref);
    std::fs::remove_file(&path_to_out);
    // Make a vector to hold the children which are spawned.
    let mut children = vec![];
    let input_file:  File = match File::open(path_to.clone()) {
        Ok(f) => f,
        Err(error) => {
            panic!("Open {:?} error {:?}", path_to, error);
        }
    };

    let mut output_file:  File = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path_to_out.clone()) {
        Ok(f) => f,
        Err(error) => {
            panic!("Open {:?} error {:?}", path_to_out, error);
        }
    };
    let read_lock = Arc::new(Mutex::new(
        CountableFile {count:1, file: input_file, finish: false}));
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let write_lock = Arc::new(Mutex::new(
        CountableGzip {count:1, encoder, finish: false}));
    for thread_id in 0..thread_nums {
        let read_lock = Arc::clone(&read_lock);
        let write_lock = Arc::clone(&write_lock);
        // Spin up another thread
        children.push(thread::spawn(move || {
            let mut buffer = [0; BUFFER_SIZE];
            let mut read_size: usize;
            let thread_read_lock = read_lock.clone();
            let thread_write_lock = write_lock.clone();
            loop {
                read_size = wrapper(&thread_read_lock, thread_id, &mut buffer);
                gzip_wrapper(&thread_write_lock, thread_id, &buffer[..read_size]);
                if read_size < BUFFER_SIZE {
                    let mut num = thread_write_lock.lock().unwrap();
                    num.finish = true;
                    //println!("{} exit thread", thread_id);
                    return ;
                }
            }
        }));
    }

    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }

    output_file.write_all({
        let a = Arc::try_unwrap(write_lock).unwrap().into_inner().unwrap();
        //let num = write_lock.lock().unwrap();
        a.encoder.finish().unwrap().as_slice()
    }).unwrap();
}
