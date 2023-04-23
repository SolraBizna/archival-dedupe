use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry as HashMapEntry},
    fs::{File, Metadata},
    io,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    thread::{Builder as ThreadBuilder, JoinHandle},
    time::{Instant, Duration},
};

use clap::Parser;
use crossbeam_channel::{Sender, Receiver, bounded};
use liso::{liso, Response};

type Checksum = [u8; 32];

const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(500);

enum ChecksumGet {
    Unrequested,
    Ungotten(Receiver<io::Result<Checksum>>),
    Gotten(Checksum),
    Failed(io::Error),
}

impl ChecksumGet {
    fn get(&mut self) -> Result<Checksum, &io::Error> {
        loop {
            match self {
                ChecksumGet::Unrequested => panic!("Tried to get an unrequested ChecksumGet!"),
                ChecksumGet::Ungotten(rx) => {
                    let result = rx.recv().unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::Other, "hash thread panic(k)ed")));
                    match result {
                        Ok(sum) => *self = ChecksumGet::Gotten(sum),
                        Err(x) => *self = ChecksumGet::Failed(x),
                    }
                },
                ChecksumGet::Gotten(checksum) => return Ok(checksum.clone()),
                ChecksumGet::Failed(error) => return Err(error),
            }
        }
    }
}

struct FileInfo {
    paths: Vec<PathBuf>,
    /// inode number of file
    ino: u64,
    size: u64,
    hash: ChecksumGet,
}

struct Database {
    /// All files that we've seen so far. Maps inode number to `File`.
    all_files: HashMap<u64, FileInfo>,
    /// All inode numbers we've seen so far that were a given size.
    inodes_of_size: HashMap<u64, Vec<u64>>,
    /// The queue of directories to process.
    queued_paths: VecDeque<PathBuf>,
    /// device number of the device we're working on
    dev: u64,
    /// inode numbers of seen dirs
    seen_dirs: HashSet<u64>,
    /// all duplicates; tuple is (original size, original inode, duplicate
    /// inodes)
    found_dupes: Vec<(u64, u64, Vec<u64>)>,
    verbose: bool,
    next_status_update: Instant,
}

impl Database {
    fn new(paths: Vec<PathBuf>, verbose: bool) -> io::Result<Database> {
        if paths.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "no paths specified"));
        }
        let dev = paths[0].metadata()?.dev();
        let seen_dirs = HashSet::with_capacity(paths.len());
        let queued_dirs = paths.into();
        Ok(Database {
            seen_dirs,
            queued_paths: queued_dirs,
            all_files: HashMap::new(),
            inodes_of_size: HashMap::new(),
            found_dupes: vec![],
            dev,
            verbose,
            next_status_update: Instant::now() + STATUS_UPDATE_INTERVAL,
        })
    }
    fn scan(&mut self, io: &mut liso::InputOutput) -> Result<(), u32> {
        let num_hash_threads = num_cpus::get().saturating_sub(1).max(1);
        let (mut tx, rx) = bounded(num_hash_threads*2);
        let hash_threads: Vec<JoinHandle<()>> = (0..num_hash_threads)
            .map(|n| {
                let rx = rx.clone();
                ThreadBuilder::new().name(format!("hash {}", n+1))
                .spawn(move || { hash_worker(rx) })
                .expect("Unable to spawn thread!")
            }).collect();
        let mut processed_inodes = 0;
        let mut seen_bytes = 0;
        let mut error_count = 0;
        while let Some(path) = self.queued_paths.pop_front() {
            let now = Instant::now();
            if now >= self.next_status_update {
                while let Some(x) = io.try_read() {
                    match x {
                        Response::Dead => std::process::exit(42),
                        Response::Quit => {
                            io.wrapln("^C");
                            return Err(1);
                        },
                        _ => (),
                    }
                }
                self.next_status_update = now + STATUS_UPDATE_INTERVAL;
                io.status(Some(liso!(
                    fg=green, bg=black, +inverse,
                    format!(" {size} in {processed} / {total} ",
                        processed= processed_inodes,
                        total= processed_inodes + self.queued_paths.len() + 1,
                        size= printable_size(seen_bytes))
                )));
            }
            match self.process_path(io, &mut tx, &path, &mut seen_bytes) {
                Ok(x) => x,
                Err(x) => {
                    io.wrapln(liso!(
                        fg=red, +bold,
                        "ERROR: ",
                        fg=None, reset,
                        format!("{:?} -> {}", path, x)
                    ));
                    error_count += 1;
                    continue;
                },
            };
            processed_inodes += 1;
        }
        drop(tx);
        for (n, join_handle) in hash_threads.into_iter().enumerate().rev() {
            if !join_handle.is_finished() {
                io.status(Some(liso!(
                    fg=yellow, bg=black, +inverse,
                    format!(" Waiting for checksums ({} threads left)... ", n+1)
                )));
            }
            if let Err(x) = join_handle.join() {
                io.wrapln(liso!(
                    fg=red, +bold,
                    "ERROR: ",
                    fg=None, reset,
                    format!("worker thread {} -> {:?}", n+1, x)
                ));
                error_count += 1;
            }
        }
        io.status::<&str>(None);
        if error_count > 0 {
            Err(error_count)
        }
        else {
            Ok(())
        }
    }
    fn process_path(&mut self, io: &mut liso::InputOutput, tx: &mut Sender<(PathBuf, Sender<io::Result<Checksum>>)>, path: &Path, seen_bytes: &mut u64) -> io::Result<()> {
        let metadata = path.symlink_metadata()?;
        if metadata.is_dir() {
            // This is a directory (and not a symlink to one!)
            self.process_dir(io, tx, path, metadata)
        }
        else if metadata.is_file() {
            // This is a file (and not a symlink to one!)
            *seen_bytes += metadata.size();
            self.process_file(io, tx, path, metadata)
        }
        else {
            if self.verbose {
                io.wrapln(liso!(
                    dim,
                    format!("{:?} is a special file (ignoring)", path)
                ));
            }
            Ok(())
        }
    }
    fn process_dir(&mut self, io: &mut liso::InputOutput, _tx: &mut Sender<(PathBuf, Sender<io::Result<Checksum>>)>, path: &Path, metadata: Metadata) -> io::Result<()> {
        if metadata.dev() != self.dev {
            if self.verbose {
                io.wrapln(liso!(
                    dim,
                    format!("{:?} is a mount point (ignoring)", path)
                ));
            }
            return Ok(())
        }
        if self.seen_dirs.contains(&metadata.ino()) {
            if self.verbose {
                io.wrapln(liso!(
                    dim,
                    format!("{:?} has been seen already (ignoring)", path)
                ));
            }
            return Ok(())
        }
        if self.verbose {
            // TODO: superverbose
            //io.wrapln(format!("{:?}", path));
        }
        self.seen_dirs.insert(metadata.ino());
        for ent in path.read_dir()? {
            let ent = ent?;
            self.queued_paths.push_back(ent.path());
        }
        Ok(())
    }
    fn process_file(&mut self, io: &mut liso::InputOutput, sum_tx: &mut Sender<(PathBuf, Sender<io::Result<Checksum>>)>, path: &Path, metadata: Metadata) -> io::Result<()> {
        if metadata.size() == 0 {
            if self.verbose {
                io.wrapln(liso!(
                    dim,
                    format!("{:?} is empty (ignoring)", path)
                ));
            }
            return Ok(())
        }
        let need_queue_hash_for = match self.all_files.entry(metadata.ino()) {
            HashMapEntry::Occupied(mut entry) => {
                entry.get_mut().paths.push(path.to_owned());
                None
            },
            HashMapEntry::Vacant(entry) => {
                if self.verbose {
                    // TODO: superverbose
                    //io.wrapln(format!("{:?}", path));
                }
                let (hash, need_queue_hash_for) = match self.inodes_of_size.entry(metadata.size()) {
                    HashMapEntry::Occupied(mut entry) => {
                        let other = if entry.get().len() == 1 {
                            Some(entry.get()[0])
                        } else { None };
                        entry.get_mut().push(metadata.ino());
                        let (result_tx, result_rx) = bounded(1);
                        let _ = sum_tx.send((path.to_owned(), result_tx));
                        (ChecksumGet::Ungotten(result_rx), other)
                    },
                    HashMapEntry::Vacant(entry) => {
                        entry.insert(vec![metadata.ino()]);
                        (ChecksumGet::Unrequested, None)
                    },
                };
                entry.insert(FileInfo {
                    paths: vec![path.to_owned()],
                    ino: metadata.ino(),
                    size: metadata.size(),
                    hash: hash,
                });
                need_queue_hash_for
            },
        };
        if let Some(ino) = need_queue_hash_for {
            let file = self.all_files.get_mut(&ino).expect("forgot an inode!?");
            if let ChecksumGet::Unrequested = file.hash {
                let (result_tx, result_rx) = bounded(1);
                let _ = sum_tx.send((file.paths[0].to_owned(), result_tx));
                file.hash = ChecksumGet::Ungotten(result_rx);
            }
            else {
                panic!("already requested/got a hash for a file that we thought we didn't yet!?");
            }
        }
        Ok(())
    }
    fn resolve(&mut self, io: &mut liso::InputOutput) -> Result<(),usize> {
        let mut error_count = 0;
        for (n, (_size, inodes)) in self.inodes_of_size.iter().enumerate() {
            let now = Instant::now();
            if now >= self.next_status_update {
                while let Some(x) = io.try_read() {
                    match x {
                        Response::Dead => std::process::exit(42),
                        Response::Quit => {
                            io.println("^C");
                            return Err(1);
                        },
                        _ => (),
                    }
                }
                self.next_status_update = now + STATUS_UPDATE_INTERVAL;
                io.status(Some(liso!(
                    fg=green, bg=black, +inverse,
                    format!(" Comparing {}/{} ", n, self.inodes_of_size.len())
                )));
            }
            if inodes.len() <= 1 { continue }
            // Possible duplicates.
            let sums: Result<Vec<(u64, Checksum)>, ()> = inodes.iter().map(|ino| {
                match self.all_files.get_mut(ino).unwrap().hash.get() {
                    Ok(x) => Ok((*ino, x)),
                    Err(x) => {
                        // thanks, non-lexical lifetimes, for saving me
                        let formatted_error = format!("{}", x);
                        io.wrapln(liso!(
                            fg=red, +bold,
                            "ERROR: ",
                            fg=None, reset,
                            format!("reading {:?}: {}", self.all_files.get(ino).unwrap().paths[0], formatted_error)
                        ));
                        error_count += 1;
                        Err(())
                    },
                }
            }).collect();
            let mut sums = match sums {
                Ok(x) => x,
                Err(()) => continue,
            };
            let mut dupes = Vec::with_capacity(sums.len());
            while sums.len() > 1 {
                dupes.clear();
                let src_sum = sums[0].1;
                for n in 1 .. sums.len() {
                    if sums[n].1 == src_sum {
                        dupes.push(n);
                    }
                }
                if dupes.len() == 0 {
                    // not a dupe
                    sums.remove(0);
                }
                else {
                    // a dupe
                    let orig_file = self.all_files.get(&sums[0].0).unwrap();
                    assert_eq!(orig_file.ino, sums[0].0);
                    self.found_dupes.push((orig_file.size, orig_file.ino, dupes.iter().map(|x| sums[*x].0).collect()));
                    for dupe in dupes.iter().rev() {
                        sums.remove(*dupe);
                    }
                    sums.remove(0);
                }
            }
        }
        if error_count > 0 {
            Err(error_count)
        }
        else {
            Ok(())
        }
    }
    fn print_report(&mut self, io: &mut liso::InputOutput, hypothetical: bool) -> Result<(),usize> {
        if self.found_dupes.is_empty() {
            io.wrapln(format!("No duplicates found among {} inodes.", self.all_files.len()));
        }
        else {
            let unique_duped_inodes = self.found_dupes.len();
            let total_duped_inodes = self.found_dupes.iter().fold(0, |a,(_,_,dupes)| {
                a + 1 + dupes.len()
            });
            io.wrapln(format!("Among {} file inodes, {} were duplicates of {} unique files.", self.all_files.len(), total_duped_inodes, unique_duped_inodes));
            let saved_space = self.found_dupes.iter().fold(0, |a,(size,_orig,dupe)| {
                a + *size * dupe.len() as u64
            });
            io.wrapln(format!("Deduping {would} save {savedbytes} bytes ({saved}).",
                would = if hypothetical { "would" } else { "will" },
                saved = printable_size(saved_space),
                savedbytes = saved_space,
            ));
        }
        Ok(())
    }
    fn dedupe(&mut self, io: &mut liso::InputOutput) -> Result<(), usize> {
        let mut error_count = 0;
        for (n, (_size, orig, dupes)) in self.found_dupes.iter().enumerate() {
            let now = Instant::now();
            if now >= self.next_status_update {
                while let Some(x) = io.try_read() {
                    match x {
                        Response::Dead => std::process::exit(42),
                        Response::Quit => {
                            io.wrapln("^C");
                            return Err(1);
                        },
                        _ => (),
                    }
                }
                self.next_status_update = now + STATUS_UPDATE_INTERVAL;
                io.status(Some(liso!(
                    fg=green, bg=black, +inverse,
                    format!(" Deduping: {} / {} ",
                        n, self.found_dupes.len()),
                )));
            }
            let (oldest_ino, _) = dupes.iter().fold((*orig, self.all_files.get(orig).unwrap().paths[0].symlink_metadata().unwrap().modified().unwrap()), |(oldest_ino, oldest_time), current_ino| {
                let current_time = self.all_files.get(orig).unwrap().paths[0].symlink_metadata().unwrap().modified().unwrap();
                if current_time < oldest_time {
                    (*current_ino, current_time)
                }
                else {
                    (oldest_ino, oldest_time)
                }
            });
            let oldest_file = self.all_files.get(&oldest_ino).unwrap();
            let oldest_path = &oldest_file.paths[0];
            let oldest_permissions = oldest_path.symlink_metadata().unwrap().permissions();
            let mut readonly_permissions = oldest_permissions.clone();
            readonly_permissions.set_readonly(true);
            if readonly_permissions != oldest_permissions {
                if let Err(x) = std::fs::set_permissions(oldest_path, readonly_permissions.clone()) {
                    io.wrapln(liso!(
                        fg=yellow, +bold,
                        "WARNING: ",
                        fg=None, reset,
                        format!("{:?}: couldn't make read only: {}", oldest_path, x)
                    ));
                }
            }
            for &ino in [orig].into_iter().chain(dupes.iter()) {
                if ino == oldest_ino { continue }
                let file = self.all_files.get(&ino).unwrap();
                for path in file.paths.iter() {
                    let metadata = path.symlink_metadata().unwrap();
                    assert_eq!(metadata.ino(), file.ino);
                    let mut permissions = metadata.permissions();
                    permissions.set_readonly(true);
                    if permissions != readonly_permissions {
                        io.wrapln(liso!(
                            fg=yellow, +bold,
                            "WARNING: ",
                            fg=None, reset,
                            format!("{:?} and {:?} were identical, but will have differing permissions!", oldest_path, self.all_files.get(&ino).unwrap().paths[0])
                        ));
                    }
                    match self.dedupe_file(io, oldest_path, path) {
                        Ok(_) => (),
                        Err(x) => {
                            io.wrapln(liso!(
                                fg=red, +bold,
                                "ERROR: ",
                                fg=None, reset,
                                format!("{:?}: {}", path, x)
                            ));
                            error_count += 1;
                        },
                    }
                }
            }
        }
        if error_count > 0 {
            Err(error_count)
        }
        else {
            Ok(())
        }
    }
    fn dedupe_file(&self, io: &mut liso::InputOutput, orig_path: &Path, new_path: &Path) -> io::Result<()> {
        if self.verbose {
            io.wrapln(liso!(
                dim, format!("{:?} ← {:?}", orig_path, new_path)
            ));
        }
        std::fs::remove_file(new_path)?;
        nix::unistd::linkat(
            None, orig_path,
            None, new_path,
            nix::unistd::LinkatFlags::NoSymlinkFollow,
        )?;
        Ok(())
    }
}

fn hash_get(path: PathBuf) -> io::Result<Checksum> {
    let mut f = File::open(path)?;
    let mut sha256 = lsx::sha256::BufSha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let red = f.read(&mut buf)?;
        if red == 0 { break }
        sha256.update(&buf[..red]);
    }
    Ok(sha256.finish(&[]))
}

fn hash_worker(rx: Receiver<(PathBuf, Sender<io::Result<Checksum>>)>) {
    while let Ok((path, tx)) = rx.recv() {
        if tx.send(hash_get(path)).is_err() {
            // An error will happen here if the main thread goes away. Silently
            // accept our fate.
            break
        }
    }
}

fn printable_size(mut bytecount: u64) -> String {
    const PREFIXES: &[&str] = &["K", "M", "G", "T", "P", "E"];
    if bytecount == 0 { return format!("--") }
    else if bytecount < 2000 { return format!("{}B", bytecount) }
    for prefix in PREFIXES.iter() {
        if bytecount < 950000 {
            return format!("{}.{:02}{}B", bytecount / 1000, (bytecount / 10) % 100, prefix);
        }
        else {
            bytecount /= 1000;
        }
    }
    // not reachable?
    format!("Lots!")
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Invocation {
    /// One or more directories to crawl or files to dedupe. They must all be
    /// within the same filesystem.
    #[arg(required=true)]
    paths: Vec<PathBuf>,
    /// If specified, will print a report and prompt whether to proceed. If not
    /// specified, will always proceed!
    #[arg(short, long, conflicts_with="dry_run")]
    interactive: bool,
    /// If specified, will print a report and exit without doing anything.
    #[arg(short, long, conflicts_with="interactive")]
    dry_run: bool,
    /// If specified, will output lots more information about what it's doing.
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let invocation = Invocation::parse();
    let mut io = liso::InputOutput::new();
    // TODO: check for dupes
    let mut db = match Database::new(invocation.paths, invocation.verbose) {
        Ok(x) => x,
        Err(x) => {
            io.wrapln(liso!(
                fg=red, +bold,
                "ERROR: ",
                fg=None, reset,
                format!("{}", x)
            ));
            io.blocking_die();
            std::process::exit(1)
        }
    };
    if db.scan(&mut io).is_err() || db.resolve(&mut io).is_err() {
        io.blocking_die();
        std::process::exit(1)
    }
    if invocation.verbose || invocation.interactive || invocation.dry_run {
        db.print_report(&mut io, invocation.dry_run || invocation.interactive).unwrap();
    }
    if invocation.dry_run || db.found_dupes.is_empty() { return }
    let proceed = if invocation.interactive {
        io.prompt(liso!("Proceed with deduplication? [yn] ", fg=cyan), true, true);
        loop {
            let response = io.read_blocking();
            match response {
                Response::Dead => std::process::exit(1),
                Response::Quit => {
                    io.println("^C");
                    break false
                },
                Response::Input(line) => {
                    io.println(liso!(
                        "Proceed with deduplication? [yn] ", fg=cyan, &line
                    ));
                    let line = line.to_ascii_lowercase();
                    if line.starts_with("n") || line.starts_with("q") {
                        break false
                    }
                    else if line.starts_with("y") {
                        break true
                    }
                    else {
                        io.wrapln("Please respond with \"yes\" or \"no\".");
                    }
                },
                _ => (),
            }
        }
    } else { true };
    if !proceed { return }
    if db.dedupe(&mut io).is_err() {
        io.blocking_die();
        std::process::exit(1)
    }
}
