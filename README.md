# normalize_audio_library

These are still very much experiemental scripts.  The design methodology errs very much on the side of caution for your files.  Losing audio is highly unlikely, but other side effects are possible.  Use at your own risk.  Contains 2 scripts.  One for generic presorting that is really just here for my own convenience and is in very early development.  The other, however, is a fairly sophisticated tool for normalizing the tags and filenames of your MP3 audio library.

## normalize.pl
The venerable normalizer script.

### Dependencies:
  * DB_File
  * Digest::MD5
  * File::Basename
  * File::Copy
  * File::Copy::Recursive
  * File::Path
  * Image::Magick
  * IO::String
  * MP3::Tag
  * Storable
  * Text::Capitalize

### Help:

    normalize_audio_library.pl

    Arguments are processed positionally. (So they must be specified in the order listed here).
    Directory options must all preceed processing options.
    Usage: normalize_audio_library.pl [DIRECTORY OPTIONS] [PROCESSING OPTIONS] [PROCESSING ACTIONS]

     -h, --help                  prints this message

    Directory Options
     -s, --search_dir=DIR        where your files to be organized live
     -r, --root_dir=DIR          where your organized files will live; if not specified
                                 process is done in-place
     -b, --backup_dir=DIR        the script makes backups for certain transforms; they go
                                 here
     -t, --total_files=INT       in case you already know and don't want to wait for it
                                 to rescan total

    Processing Options
     -u, --itunes_compat         itunes compatability mode.  follows itunes file naming
                                 pragmas for injecting into a library that lets iTunes
                                 "keep media folder organized"

    Processing Actions
     -n, --normalize_paths       clean up
     -i, --locate_lowres_images  scans for your normalized folder.jpg files and complains
                                 if they are low resolution

## autoimport-sort.pl

A file agnostic presorter.  Behavior still hardcoded into head of script -- needs to be parameterized.

