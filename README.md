# sortrec

really fast and thorough golang port of [tfrdidi/sort-PhotorecRecoveredFiles](https://github.com/tfrdidi/sort-PhotorecRecoveredFiles) which is a fork of [ChrisMagnuson/sort-PhotorecRecoveredFiles](https://github.com/ChrisMagnuson/sort-PhotorecRecoveredFiles)

### New Features

- [x] **concurrency** - process files concurrently using configurable worker pools via [panjf2000/ants](https://github.com/panjf2000/ants).
- [x] **wav file support** - restore filenames for WAV by reading the RIFF header.
- [x] **symbolic linking instead of copying** - create symbolic links instead of copying files.
- [x] **more spooky bugs to discover** - there's a saying that applies here; _"go fast, break things"_. this seems to work fine, and should be safe, but.. y'know. use with caution until test cases are introduced.
- [ ] **configurable restoration of copy functionality** - right now it's symbolic links only

## usage

flags are ported directly (with a few additions) from the original script, so they should be familiar to anyone who's used that.
    
```
Usage of ./sortrec:
-date_time_filename
Sets the filename to the exif date and time if possible
-dest string
Destination directory to write sorted files to
-keep_filename
Keeps the original filenames when copying
-max-per-dir int
Maximum number of files per directory (default 500)
-min-event-delta int
Minimum delta in days between two events (default 4)
-pools int
Number of pools to use (default 25)
-split-months
Split JPEG files not only by year but by month as well
-src string
Source directory with files recovered by Photorec
-workers int
Number of workers to use (default 5)
```
