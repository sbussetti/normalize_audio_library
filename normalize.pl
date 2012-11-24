#!/usr/bin/perl
use strict;
use utf8;
use MP3::Tag;
use Text::Capitalize qw/capitalize_title/;
use File::Copy qw/copy move/;
use Digest::MD5 qw/md5_hex/;
use File::Copy::Recursive qw/rcopy rmove dirmove/;
use File::Path qw/make_path remove_tree/;
use File::Basename;
use DB_File;
use Storable qw/freeze thaw/;
use Image::Magick;

use Encode qw/encode decode/;
use File::Spec; 
use File::Find;
use Data::Dumper qw/Dumper/;
use Getopt::Long qw/:config bundling/;
use Cwd qw//;

#use MP4::Info;  ## If they made an MP4::Tag lib, I'd use it...
#have to patch it, no less, and included it in script.. get nasty:
#ghetto selfloading..
my @libs = <main::DATA>;
eval join('', @libs);

$|=1;


#############################
#  In general there are limited heuristics in this script.  No attempts are made to "guess" a relation between tags, except for capitalization.
#   Minor variances in tags (say artist or album name) are treated as separate items.
#
# To Do:
# * Incompletes / Improperly Sequenced -- albums with non sequential track / disc numbers.
#   (some 90's albums w/ hidden tracks have a gap due to many blank intermediary tracks, could be false positive...)
# * File moves are not atomic and are not cleaned up in END -- you won't lose stuff, but you can easily end up with duplication
#   if the script is interrupted during a move operation.
# * More accurate progress.  Postprocess happens after the majority of actions, but there's also the very last pass that re-recurses down 
#   the entire tree from the root -- maybe prevent that anyway, but pre and postproc should be considered...
# * Either me or Text::Capitalize is still mucking up some unicode characters in tag names.  Unicode in filenames is OK  
#
# Testing:
# * Another duplication/validation strategy would be to count up all ocurrances of track/disc numbers for albums that match on title.
#   if done as a prostproc, we'd already know the directories would be in place and album names corrected.  
# Done:
# * Albums with same name but varying capitalization.  Should be fixed before casemove 
#   operations or else you end up with an endless rename cycle on subsequent runs.
#
# Working:
# * iTunes compat:  Album Artist/Album/[disc]-Track Title.ext
#                   * max 40 characters for each segment (including extension and track number..)
#                   * replace leading (period?) punctuation with underscore...
#                   * itunes is less restrictive of foreign characters in filenames..
#                       e.g. the Delta character in LAKE RADIO or the accented o in Sigur Ros or a in Joao Gilberto..
#                   * compilation flag -- if set iTunes puts the album in artist folder "Compilations"
# * M4A/MP4/AAC support: Capable of reading AAC tags, but does not support writing them. 
#
############################
#
# SET UP GLOBALS AND GLOBAL CONFIG..
my $DIR = {};
my $BACKUP_DIR = '';
my $ROOT_DIR = '';
my $SEARCH_DIR = '';
my $FILE_COUNT = 0;
my $TOTAL_FILES = 0;
my $ITUNES_COMPAT = 0;
my $OS = 'UNIX';

my $DISPATCHED = 0; ## true if we successfully dispatched an action..

my @LEAVE_DIRS = ();
my $TAGS = {};
my $DIRALBTRACKS = {};
my @EMPTYDIRS = ();
my $z = tie my %FILE_HASH, 'DB_File', '/tmp/file_hash.db', O_RDWR|O_CREAT, 0666, $DB_HASH
        or print STDERR "cannot open database file: $!\n" and exit;

MP3::Tag->config(write_v24 => 'TRUE'); ## enable "mostly acceptable" ID3v2.4 writing

## EXIT HANDLER
sub cleanup {
    print "\nExiting... Clean up\n";
    foreach my $HH ( $z ) {
        $HH->sync && untie $HH;
    }
}

$SIG{INT} = sub { exit };
END { &cleanup }

GetOptions(
    'help|h' => \&help,

    'total_files|t=i' => \$TOTAL_FILES,  ## in case you know and don't want to wait for a scan
    'search_dir|s=s' => \$SEARCH_DIR,   ## where your files to be organized live
    'root_dir|r=s' => \$ROOT_DIR,     ## where you want organized files to end up.  If not specified, fix is done in-place
    'backup_dir|b=s' => \$BACKUP_DIR, ## where you want backups to go of modified files..

    'itunes_compat|u' => \$ITUNES_COMPAT,

    'locate_duplicates|d' => \&dispatch,  ## NOT VERY SMART YET
    'list_duplicates|l' => \&dispatch,    
    'normalize_paths|n' => \&dispatch,
    'locate_lowres_images|i' => \&dispatch,  ## STILL UNDER DEVELOPMENT...
);

help() and exit 0 if not $DISPATCHED;

sub dispatch {
    my $opt = (shift);
    print STDERR "Must specify at least a search dir\n" and exit unless defined $SEARCH_DIR and $SEARCH_DIR ne '';
    foreach ($ROOT_DIR, $SEARCH_DIR, $BACKUP_DIR) {
        $_ = Cwd::abs_path($_) if (defined $_ and $_ ne '');
    }
    $ROOT_DIR = $SEARCH_DIR unless defined $ROOT_DIR and $ROOT_DIR ne '';

    $DIR = {
        ROOT => File::Spec->canonpath($ROOT_DIR),
        SEARCH => File::Spec->canonpath($SEARCH_DIR),
        BACKUP => File::Spec->canonpath($BACKUP_DIR),
    };

    ## ensure releif dirs exist
    foreach my $d ($BACKUP_DIR) {
        make_path($d) unless -d $d;
    }
    
    print Dumper($DIR), "\n";
    my $map = {
        'locate_duplicates' => \&locate_duplicates,
        'list_duplicates' => \&list_duplicates,
        'normalize_paths' => \&normalize_paths,
        'locate_lowres_images' => \&locate_lowres_images
    };
    $DISPATCHED = 1;
    $map->{$opt}->();
}
sub help {
    print q[
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
];
 #-d, --locate_duplicates     this tries to identify duplicates and prints out a report
 #                            about them
 #-l, --list_duplicates       display results of the previous run of --locate_duplicates
}

sub setup_file_count_globals {
    my ($type) = (shift, );
    print "\nCounting audio related files in $DIR->{SEARCH}...\n";
    if (! $TOTAL_FILES) {
        if ($type eq 'jpg') {
            find({wanted => \&wanted__filecount_jpg_finder}, $DIR->{SEARCH});
        } else {
            find({wanted => \&wanted__filecount_mpx_finder}, $DIR->{SEARCH});
        }    
    }
    print "\nFound $TOTAL_FILES to process.  beginning search...\n";
}
sub wanted__filecount_mpx_finder { 
        next unless -f && /^(.*\.(mp[34]a?|m4a|aac))$/i; $TOTAL_FILES++; print "\rFound: $TOTAL_FILES"; _flush(*STDOUT); 
}
sub wanted__filecount_jpg_finder { 
        next unless -f && /^(.*\.jpg)$/i; $TOTAL_FILES++; print "\rFound: $TOTAL_FILES"; _flush(*STDOUT); 
}

sub locate_duplicates {
    setup_file_count_globals;

    find({
            wanted => \&wanted__locate_duplicates,
    }, $DIR->{SEARCH});

}
sub wanted__locate_duplicates {
    next unless -f && /^(.*\.(mp[34]a?|m4a|aac))$/i; 

    $FILE_COUNT++;
    _show_progress($FILE_COUNT/$TOTAL_FILES);
    my $md5; 
    my $hkey = uc($File::Find::name);
    if ( ! exists $FILE_HASH{$hkey} ) {
        $md5 = _md5sum($File::Find::name);
        print STDERR "NO HEX DIGEST OF $File::Find::name\n" and next if ( ! $md5 );

        print STDERR "NEW: $hkey ($md5)\n";
        
        $FILE_HASH{$hkey} = $md5;
    } else {
        $md5 = $FILE_HASH{$hkey};
        print STDERR "CACHED: $hkey ($md5)\n";
    }
    print STDERR "######### SYNC #########\n" and $z->sync if ! ( $FILE_COUNT % 100 );
}
sub list_duplicates {
    my %SEEN_HASH = ();
    my %MULTI_KEYS = ();  ## once there's more than one we need to note it.
    foreach my $key (keys %FILE_HASH) {
        if ( ! exists $SEEN_HASH{$key} ) {
            print STDERR "FIRST: $FILE_HASH{$key} <> $key\n";
            $SEEN_HASH{$FILE_HASH{$key}} = [ $key ];
        } else {
            $MULTI_KEYS{$FILE_HASH{$key}} = 1;
            push @{$SEEN_HASH{$FILE_HASH{$key}}}, $key;
            print STDERR "WILL MULTI: $FILE_HASH{$key} <> $key\n";
        }
    }

    foreach my $mkey (keys %MULTI_KEYS) {
        my @exact_files = @{$SEEN_HASH{$mkey}};
        foreach my $dupe (@exact_files){
            print STDERR "\n\n\n", "$dupe\n";
        }
    }
}

sub locate_lowres_images {
    setup_file_count_globals('jpg');

    find({
            wanted => \&wanted__locate_lowres_images,
    }, $DIR->{SEARCH});
}
sub wanted__locate_lowres_images {
    ## is more or less a post-process step for normalize_paths
    ## and will likely become one once it's finished
    ## we're checking our work here, as well as the quality of the source
    ## indirectly..
    my ($name, $ext) = m/^(folder).(jpg)$/i;
    next unless $ext;

    $FILE_COUNT++;
    _show_progress($FILE_COUNT/$TOTAL_FILES);


    my $img = Image::Magick->new;
    $img->Read($File::Find::name);

    my ($format, $depth, $density, $quality) = $img->Get('magick', 'depth', 'density', 'quality');
    my @density = $density =~ m/(\d)x(\d)/;
    my $qual = 'HIGHQUAL';
    if ($format ne 'JPEG' or @density[0] < 300 or @density[1] < 300 or $quality < 75) {
        $qual = 'LOWQUAL';
        #my $mp3 = MP3::Tag->new("$File::Find::name");
        #$mp3->update_tags({Conductor => 'BAD ART'});
        #$mp3->update_tags;
        print STDERR "$qual: $File::Find::name )) $format: Depth: $depth, Density: $density, Quality: $quality \n";
    }
}

sub normalize_paths {
    setup_file_count_globals;

    ### LEAVE DIRS IS A JOBLIST.  AT FIRST IT IS THE SEARCH ROOT
    ### BUT THEN EACH SEARCH PASS EMPTIES IT BEFORE PROCESSING AND MAY
    ### ADD MORE DIRS TO BE REPROCESSED...
    @LEAVE_DIRS = ($DIR->{SEARCH});
    while (@LEAVE_DIRS) {
        my @CURRENT_JOB = @LEAVE_DIRS;
        @LEAVE_DIRS = ();
        find({
            preprocess => \&preprocess__normalize,
            wanted => \&wanted__normalize,
            postprocess => \&postprocess__normalize
        }, @CURRENT_JOB);
        ## remove our list of empty directories..
        ## can't do it as we go or File::Find will cry
        foreach my $dir (@EMPTYDIRS) {
            my $e = basename($dir);

            if ($DIR->{BACKUP}) {
                my $tp = $DIR->{BACKUP}.'/'.$e ;
                my $md = 0;
                do { $md++ } while ( -e $tp.($md ? '.'.$md : ''));
                print STDERR "Removing empty dir: $dir\n";
                my $emptyd = $tp.($md ? '.'.$md : ''); 
                if ($OS eq 'WINDOWS') {
                    _casemove($dir, $emptyd) or die "Failed to move: $dir => $emptyd";
                } else {
                    dirmove($dir, $emptyd) or die "Failed to move: $dir => $emptyd";
                }
            } else { #delete
                remove_tree($dir) or die "Failed to delete: $dir";
            }
        }
        @EMPTYDIRS = ();
    }
}
sub wanted__normalize {

    ## moves audio files only to paths following their album / artist / track names
    ## ignores all other files, except folder.jpg...
    my ($name, $ext) = m/^(.*)\.(mp[34]a?|m4a|aac)$/i;
    my $File__Find__name = encode('utf8', decode('utf8', $File::Find::name));
    my $File__Find__dir = encode('utf8', decode('utf8', $File::Find::dir));
    next unless $ext and -f $File__Find__name;

    $FILE_COUNT++;
    _show_progress($FILE_COUNT/$TOTAL_FILES);

    my ($tag, $mp3, $mp4) = @{$TAGS->{_remove_extended_chars(uc($File__Find__name))}};
    next if not $tag;  ## tag parser will bitch to logs about what's wrong..

    my $SKIP_ON_MISSING_TAG = 0;
    foreach ( keys %$tag) {
        if ($OS eq 'WINDOWS') {
            $tag->{$_} = _remove_extended_chars($tag->{$_});
        }
        #this one is good! removes unsafe characters!!!
        $tag->{$_} =~ s/([\/\\\*\|\:"\<\>\?]|^\.|\.$)/_/g;
        $SKIP_ON_MISSING_TAG = "$_ is blank" and last if (not /^(disk|track)$/ and /^\s*$/) or not defined $_;
    }
    print STDERR "ERROR: Field $SKIP_ON_MISSING_TAG\n" && next if $SKIP_ON_MISSING_TAG;

    my $md5;
    if ( ! exists $FILE_HASH{_remove_extended_chars(uc($File__Find__name))} ) {
        $md5 = _md5sum($File::Find::name);
        if ( ! $md5 ) {
            print STDERR "NO HEX DIGEST OF $File__Find__name\n";
            return;
        }
        $FILE_HASH{_remove_extended_chars(uc($File__Find__name))} = $md5;
    } else {
        $md5 = $FILE_HASH{_remove_extended_chars(uc($File__Find__name))};
    }    

    my @pparts = ();
    if ($ITUNES_COMPAT){
        @pparts = map { $_ = substr($_, 0, 40); s/(^\s+|\s+$)//g; $_  } ( ( $tag->{album_artist} || $tag->{artist} ), $tag->{album} );
    } else {
        @pparts = ( ( $tag->{album_artist} || $tag->{artist} ), $tag->{album} );
        if ( $tag->{'disk'} ) {
            push @pparts, sprintf('Disc %s', $tag->{'disk'});
        }
    }

    my $new_relpath = join '/', @pparts;

    my $new_basename = undef;
    my $track_ph = $tag->{track} =~ /[^0-9]/ ? '%s' : '%02d';
    if ($ITUNES_COMPAT) {
        ## itunes optionally prefixes with disc number if there is a disc flag.
        ## more precisely it only does this if the album has more than one disc
        my $disks = {};
        for my $afile (keys %$TAGS){
            my ($tag, $mp3, $mp4) = @{$TAGS->{$afile}};
            $disks->{$tag->{'disk'}} = 1;
        }        
        my @disks = keys %$disks;

        my $disk = undef;
        if ($tag->{'disk'} && @disks > 1 ) {
            my ($this_disk, $total_disks) = m~^(\d+)/(\d+)$~;
            ## itunes strips leading zeros from disks, if present..
            s/^0+// foreach ($this_disk, $total_disks);
            if ($total_disks > 1) {
                $disk = $this_disk;
            } else { ## we have disk, but it doesn't match the #/# pattern, alternative is single disk enumeration..
                $disk = $tag->{'disk'};
                $disk =~ s/^0+//;
            }
        }
        my $max_title_length = 40 - length('.'.$ext) - ($disk ? length($disk)+1 : 0) - length(sprintf('%02d', $tag->{track}));
        my $ititle = substr $tag->{title}, 0, $max_title_length;
        $ititle =~ s/\s+$//;
        
        if ($disk) {
            $new_basename = sprintf '%d-'.$track_ph.' %s.'.$ext, $disk, $tag->{track}, $ititle;
        } else {
            $new_basename = sprintf $track_ph.' %s.'.$ext, $tag->{track}, $ititle;
        }
    } else {
        $new_basename = sprintf '%s - '.$track_ph.' - %s.'.$ext, $tag->{artist}, $tag->{track}, $tag->{title};
    }
    ##directory prep...
    my $full_path = File::Spec->catpath(undef, $DIR->{ROOT}, $new_relpath);
    ## if target exists, we're okay
    ## because files will be moved and empty dir cleaned..
    ## this really needs to get moved into the preprocess now that 
    ## we precache tags.. doesn't belong here and wastes cycles..
    if ((not -d $full_path) and 
        uc($File__Find__dir) eq uc($full_path) and
        $File__Find__dir ne $full_path
        ) { ## case issues -- really here we should be tracking an rebuilding @_ ... 
            my @oldparts = split /\//, $File::Find::dir;
            my @newparts = split /\//, $full_path;

            #for ( my $i = $#oldparts ; $i >= 0; $i-- ) {
            while (@oldparts and @newparts) {
                my $olddir = File::Spec->catdir('/', @oldparts);
                my $newdir = File::Spec->catdir('/', @newparts);
                ## once we've hit the top of the search dir, we're done...
                last if $DIR->{SEARCH} eq substr($olddir, 0, length($DIR->{SEARCH}));

                my ($a, $b) = (pop @oldparts, pop @newparts);
                if ( $a ne $b) {  ## again cap error.., 
                    print STDERR "MOVE PATH: $olddir => $newdir\n";
                    if ($OS eq 'WINDOWS') {
                        _casemove($olddir, $newdir);
                    } else {
                        move($olddir, $newdir) or die "Could not rename miscapitalized directory.";
                    }

                    my $oldtop = File::Spec->catdir('/', @oldparts);
                    push @LEAVE_DIRS, $oldtop;
                    ## it's messed up, up to the highest level one 
                    #  we had to move... so rescan it
                    print STDERR "LEAVE DIR: $File__Find__dir >> $oldtop\n";
                }
            }
            ##the whole movie's flawed...
            ##e.g. we just jacked a directory that likely still has files in it
            ##on file::find's stack, so we need to totally skip it.
            ##not appropriate for pre-proc since it's a dir action based on file data...
            return;
    } elsif ( ! -d $full_path ) {
        make_path( $full_path ) or die $@;
    }
    my $full_pathname = File::Spec->catpath("", $full_path, $new_basename);
    my $pathlen = length($full_pathname); 
    if ( $full_pathname eq $File__Find__name ) {
        #no action..., same path
        
        return;

    } elsif ( -e $full_pathname && ! uc($full_pathname) eq uc($File__Find__name ) ) {
        ## same path.  if it's a capitalization difference, move.. since exacts already bailed..
        #unless of course, the target file which already exists is actually the same file..
        my $target_md5;
        if ( ! exists $FILE_HASH{_remove_extended_chars(uc($full_pathname))} ) {
            $target_md5 = _md5sum($full_pathname);
            if ( $target_md5 ) {
                $FILE_HASH{_remove_extended_chars(uc($full_pathname))} = $target_md5;
            }
        } else {
            $target_md5 = $FILE_HASH{_remove_extended_chars(uc($full_pathname))};
        }

        ##done caching, do the compare here..  I guess if the target exists but we can't get a sum we overwrite it..
        if ( $target_md5 && $md5 ne $target_md5 ) {  ## not the same file after all, so skip for now..
            print STDERR "\nFILE EXISTS: $File__Find__name => $full_pathname\n skipping...\n";
            return;
        }
    } elsif ( $pathlen > 257 ) {
        ##  if we can fix it by trimming the track name, super, but lets not start abbreviating paths.. I guess?
        ##  would have to come up with a scheme for equalizing between path parts, with a max len for Artist, Album, [Disc] and Track parts

        my ($artist, $track, $title) = split ' - ', $new_basename;
        my $delta = $pathlen - 257;
        my $titlelen = length($title);
        if ($delta < $titlelen) { ## we can fix this by shortening the title..
            $title = substr($title, 0, ($titlelen - $delta)); 
            $new_basename = join ' - ', $artist, $track, $title;
        } else {
            print STDERR "\nPATH TOO LONG: $full_pathname\n skipping...\n";
            return;
        }
    }

    print STDERR "MOVE: ", $File__Find__name, " => ", $full_pathname, "\n";

    if ( uc($File__Find__name) eq uc($full_pathname) ) {
        if ($OS eq 'WINDOWS') {
            _casemove( $File::Find::name, $full_pathname ) or die "Failed to move: $File__Find__name => $full_pathname";
        } else {
            move( $File::Find::name, $full_pathname ) or die "Failed to move: $File__Find__name => $full_pathname";
        }
    } else {
        move( $File::Find::name, $full_pathname ) or die "Failed to move: $File__Find__name => $full_pathname";
    }

    delete $FILE_HASH{_remove_extended_chars(uc($File__Find__name))};
    $FILE_HASH{_remove_extended_chars(uc($full_pathname))} = $md5;

    if ( -e $File::Find::dir.'/folder.jpg' && "$File__Find__dir" ne $full_path ) {
        move ( "$File::Find::dir/folder.jpg", "$full_path/folder.jpg" ) or die $@;
        print STDERR "MOVE ART: $File__Find__dir/folder.jpg => $full_path/folder.jpg\n";
    }

    
}

sub postprocess__normalize {
    ## recursing is pointless b/c we're in a DFS..
    if ( $File::Find::dir ne $DIR->{SEARCH} && ! _recurse_for_empty($File::Find::dir) ) {
        ## turtles all the way down
        #print STDERR "EMPTY: ", $File::Find::dir, "\n";
        push @EMPTYDIRS, $File::Find::dir;
    }
=head1
    $DIRALBTRACKS = {};
    _postprocess__count_album_tracks;
    foreach my $art (keys %$DIRALBTRACKS){
        foreach my $alb (keys %{$DIRALBTRACKS->{$art}}){
            foreach my $disc (keys %{$DIRALBTRACKS->{$art}->{$alb}}){
                foreach my $track (keys  %{$DIRALBTRACKS->{$alb}->{$disc}}){
                    if ($DIRALBTRACKS->{$alb}->{$disc}->{$track} > 1) {
                        print STDERR "PROBABLE DUPLICATE ON: $art $alb $disc $track\n";
                    }
                }
            }
        }
    }
=cut
}
=head1
sub _postprocess__count_album_tracks {
    if ( $File::Find::dir ne $DIR->{SEARCH}) {
        print STDERR "PCAT: $File::Find::dir\n";
        foreach my $f ( @_ ) {
            my $lf = $File::Find::dir.'/'.$f;
            print STDERR "PCAT: SUB $lf\n";
            my $File__Find__name = encode('utf8', decode('utf8', $lf));
            next if $f =~ /^\.{1,2}$/ || ! -f $lf || $f !~ /\.(mp[34]a?|m4a|aac)$/i;
            
            my ($tag, $mp3, $mp4) = @{$TAGS->{_remove_extended_chars(uc($File__Find__name))}};

            my $disk = ( $tag->{'disk'} || 1 );
            my $artist = ( $tag->{'album_artist'} || $tag->{'artist'} );
            my $track = int $tag->{'track'};
            $DIRALBTRACKS->{$artist}->{$tag->{'album'}}->{$disk}->{$track}++;
        }
    }
}
=cut

sub preprocess__normalize {
    @_ = _preprocess__cache_tags(@_);
    @_ = _preprocess__fix_album_artist(@_);
    @_ = _preprocess__find_album_art(@_);
}
sub _preprocess__cache_tags {
    $TAGS = {};  # free up some references for garbage collector..
    foreach my $f ( @_ ) {
        my ($name, $ext) = $f =~ m/^(.*)\.(mp[34]a?|m4a|aac)$/i;
        my $lf = $File::Find::dir.'/'.$f;
        next unless $ext and -f $lf;
        my $File__Find__name = encode('utf8', decode('utf8', $lf));
        $TAGS->{_remove_extended_chars(uc($File__Find__name))} = [_get_tags($lf, $ext)];
    }
    return @_;
}
sub _preprocess__fix_album_artist {
    return @_ unless keys(%$TAGS);
    my (@albums, @album_artists, @artists);
    foreach my $key (keys %$TAGS){
        my ($tag, $mp3, $mp4) = @{$TAGS->{$key}};
        push @albums, $tag->{album};
        push @album_artists, $tag->{album_artist};
        push @artists, $tag->{artist};
    }
    foreach (\@albums, \@album_artists, \@artists) {
        my %seen = ();
        my @u = grep { ! $seen{ $_ }++ } @{$_};
        @{$_} = @u;
    }
    my $identical_artists = (@artists == 1 ? 1 : 0);
    my $identical_albums = (@albums == 1 ? 1 : 0);
    my $identical_album_artists = (@album_artists == 1 ? 1 : 0);
    #print "ART $identical_artists, ALB: $identical_albums, ARTALB: $identical_album_artists\n\n";
    my $album_mask = {};
    if ($albums[0] =~ /\bsplit\b/i and @artists == 2) {
        if (not $identical_artists and $identical_albums) {
            $album_mask->{album_artist} = join ' / ', @artists;
        }
    } else {
        if (not $identical_artists and $identical_albums and not $identical_album_artists){
            $album_mask->{album_artist} = 'Various Artists';
            $album_mask->{compilation} = 1;
        }
    }

    foreach my $f ( @_ ) {
        my $lf = $File::Find::dir.'/'.$f;
        my $File__Find__name = encode('utf8', decode('utf8', $lf));
        ## we can't write to mp4s so this only runs on mp3s...
        next if $f =~ /^\.{1,2}$/ || ! -f $lf || $f !~ /\.mp3$/i;
        
        my ($tag, $mp3, $mp4) = @{$TAGS->{_remove_extended_chars(uc($File__Find__name))}};


        my $update = {};
        ## fill in blank album artist..
        if ( ! defined $tag->{'album_artist'} || $tag->{'album_artist'} =~ /^\s*$/ ) {
            $update->{'album_artist'} = $tag->{'artist'};
        }

        my $fix = {
            album => capitalize_title($tag->{album}, PRESERVE_ALLCAPS => 1),
            album_artist => capitalize_title((defined $album_mask->{album_artist} ? $album_mask->{album_artist} : $tag->{album_artist}), PRESERVE_ALLCAPS => 1),
            artist => capitalize_title($tag->{artist}, PRESERVE_ALLCAPS => 1),
            compilation => (defined $album_mask->{compilation} ? $album_mask->{compilation} : $tag->{compilation}),
        };
        foreach my $k (qw/artist album_artist/) { 
            $fix->{$k} =~ s/(.+),\s+The\s*$/The $1/i;
        }

        foreach my $k ( keys %$fix ) {
            if ($tag->{$k} ne $fix->{$k}) {
                print $tag->{$k}, ' ne ', $fix->{$k}, "\n";
                $update->{$k} = $fix->{$k};
            }
        }

        ## update cache
        @{$tag}{keys %$update} = values %$update;

        ## write out
        if (my @uk = keys %$update) {
            my @pairs = map { "([$_] $update->{$_})" } @uk;
            print STDERR "Updating tags ".join(' ',@pairs)."\n";
            
            if (exists $update->{'album_artist'}) {
                $mp3->set_id3v2_frame('TPE2', $update->{'album_artist'});
                delete $update->{'album_artist'};
            }
            if (exists $update->{'compilation'}) {
                my $id3 = $mp3->{ID3v2};
                my $comp_frame = $id3->version < 3 ? 'TCP' : 'TCMP';
                $mp3->set_id3v2_frame($comp_frame, $update->{'compilation'});
                delete $update->{'compilation'};
            }
            $mp3->update_tags($update);
            $mp3->update_tags;
        }
        $mp3->close;
    }
    return @_;     
}
sub _preprocess__find_album_art {

    foreach my $dir ( @_ ) {
        my $ldir = $File::Find::dir.'/'.$dir;

        my @parts = split /\//, $ldir;

        #print @parts."\n";
        shift @parts;
        next unless $dir !~ /^\.{1,2}$/;  #manually look in every artist folder..

        if ( @parts == 7 || @parts == 8 ) { ## album..
            opendir PDIR, $ldir;
            my @poss = ();
            while ( local $_ = readdir(PDIR) ) { 
                next if ! /\.(jpg|png|bmp)$/i;
                push @poss, $_;
            }
            closedir PDIR;
            if ( @poss ) {
                if ( @poss == 1 ) { #simple, only one image..
                    my ($base,$ext) = $poss[0] =~ m/(.+)\.(.{3})$/;
                    next if $poss[0] eq 'folder.'.$ext; ## already matched how we want..
                    _move_art($ldir, $poss[0], 'single art');
                } else {
                    my @front_large = grep { /(front|large|cover|folder|outside|Album\s*Art)/i } @poss;

                    my $img = Image::Magick->new;
                    my $topa = 0;
                    my @topis = ();
                    foreach my $p ( @poss ) {
                        $img->Read( "$ldir/$p" );
                        my ($w,$h) = $img->Get('width','height');
                        my $a = $w*$h;
                        if ( $a > 0 && $a >= $topa ) { #- 50 ) {
                            push( @topis, $p); 
                            $topa = $a ;
                        }
                    }

                    if ( @front_large || @topis ) {
                        my %fl = map { $_ => 1 } @front_large;
                        my @best = grep { $fl{$_} } @topis;
                        if ( @best ) {
                            if ( @best == 1 ) { #easy
                                _move_art($ldir,$best[0],'best art');
                            } elsif ( ! grep { /^folder\.jpg$/i } @best ) {
                                if ( ( grep { /AlbumArt/ } @best ) == @best ) {
                                    _move_art($ldir,$best[0],'best art');
                                } elsif ( my @conv = grep { /^folder\.(png|bmp|jpg)$/i } @best ) {
                                    _move_art($ldir,$conv[0],'best art, needs conversion');
                                } elsif ( my @other = grep { /.*(front).*\.(png|bmp|jpg)$/i } @best ) {
                                    _move_art($ldir,$other[0],'best art, needs conversion');
                                } else {
                                    print STDERR "BEST: $ldir\n\t".join( "\n\t", @best )."\n";
                                }

                            }
                        } else {
                            if ( @front_large ) {
                                print STDERR "FRONT-LARGE: $ldir\n\t".join( "\n\t", @front_large )."\n";
                            } 
                            if ( @topis ) {
                                my $album_name = $parts[6];
                                my @albumn = grep { /$album_name/ } @topis;
                                if ( @albumn ) {
                                    _move_art($ldir,$albumn[0],'matched album name');   
                                } else {
                                    print STDERR "TOP IMAGES: $ldir\n\t".join( "\n\t", @topis )."\n";
                                }
                            }
                        }
                    } else {
                        print STDERR "$ldir\n\t".join( "\n\t", @poss )."\n";
                    }
                }
            }
        }
    }

    @_;
}

sub _identical {
    my $ref = shift;
    return 1 unless @$ref;
    my $cmp = $ref->[0];
    my $equal = defined $cmp ?
        sub { defined($_[0]) and $_[0] eq $cmp } :
        sub { not defined $_[0] };
    for my $v (@$ref){
        return 0 unless $equal->($v);
    }
    return 1;
}
sub _get_tags {
    my ($file, $ext) = (shift, shift);
    my ($tag, $mp3, $mp4);
    if ($ext =~ /(mp4|m4a|aac)/i) {
        my $mp4;
        eval {$mp4 = new MP4::Info $file};
        print STDERR "\nCould not parse tags from MP4: $file ($@)\n" and return () if $@;
    
        ## monkeypatch in allowing direct access to albumartist..
        $mp4->{_permitted}->{'AART'} = 1;

        $tag = {
            'album' => $mp4->ALB,
            'artist' => $mp4->ART,
            'album_artist' => $mp4->{AART},
            'disk' => $mp4->DISK,
            'title' => $mp4->NAM,
            'track' => $mp4->TRKN,
            'grouping' => $mp4->GRP,
            'compilation' => $mp4->CPIL or 0,
            '_apple_store_id' => $mp4->APID,
        };
        ## basically if disk or track are numeric 0, then just blank it out
        $tag->{'disk'} = (${$tag->{'disk'}}[0] or undef) if (ref $tag->{'disk'} eq 'ARRAY');
        $tag->{'track'} = (${$tag->{'track'}}[0] or undef) if (ref $tag->{'track'} eq 'ARRAY');
    } elsif ($ext =~ /mp3/i) {
        $mp3 = MP3::Tag->new($file) or die "Cannot parse Tags for: $file";
        $mp3->get_tags();
        my $id3 = $mp3->{ID3v2};
        ## this auto-transfers id3v1
        unless ($id3) {
            print STDERR "Auto-transfering ID3v1 info for $file\n";
            my $id31 = $mp3->{ID3v1};
            print STDERR "ID3v2 and 1 NOT AVAILABLE FOR: $file\n" and return () unless $id31;
            my $extract = {};
            for my $t (qw/artist album title year comment track genre/) {
                my $tag = $id31->$t;
                $extract->{$t} = $tag if defined $tag and $tag ne '';
            }
            $mp3->new_tag('ID3v2');
            $mp3->update_tags($extract);
            $mp3->update_tags;
            $id3 = $mp3->{ID3v2};
        }
        print STDERR "ID3 NOT AVAILABLE FOR: $file\n" and return () unless $id3;

        ## actually populate tags
        eval{ $tag = $mp3->autoinfo(); };
        print STDERR "\nBAD MP3 TAG: $file\n" && return () if $@;
        $tag->{'disk'} = $mp3->disk1;
        $tag->{'album_artist'} = $id3->get_frame('TPE2');

        ## handle N/Y track format, and discard track count which never gets used
        my ($trackn, $total_tracks);
        if (($trackn, $total_tracks) = $tag->{'track'} =~ m/^(\d+)\/(\d+)$/) {
            $tag->{'track'} = $trackn;
        }

        my $comp_frame = $id3->version < 3 ? 'TCP' : 'TCMP';
        $tag->{'compilation'} = $id3->get_frame($comp_frame) or 0;

        ## some tags have genre data in their TIT1 frame
        ## that'd be fine, but MP3::Info concatenates these fields.. (sucks!)
        my $tit1 = $id3->get_frame('TIT1');
        if (defined $tit1 and $tit1 ne '') {
            $tag->{'title'} = $id3->get_frame('TIT2');
        }
    }
    foreach (keys %$tag) {
        $tag->{$_} = encode('utf8', $tag->{$_});
    }
    return ($tag, $mp3, $mp4);
}
sub _recurse_for_empty {
    my ($ddir, $depth) = (shift, shift);
    my $base = basename($ddir); 
    #print   $depth;
    #print "\t" foreach (0..$depth+1);
    #print        "$base contains:\n";
    
    next unless -d $ddir;  ## we may have moved a directory for capitalization issues..

    opendir my $RDIR, $ddir or die "$ddir >> $!";
    my @check_dirs = grep { ! /^\.{1,2}$/ } readdir $RDIR;
    closedir $RDIR;


    my $content_count = 0;
    foreach my $d ( @check_dirs ) {
        if ( -d $ddir.'/'.$d ) {
            $content_count += _recurse_for_empty($ddir.'/'.$d, $depth+1);
        } elsif ( $d =~ /^((._)?.DS_Store|.*\.(cue|sfv|doc|nfo|log|m3u|ini|db|jpe?g|png|bmp|txt))$/i || $d !~ /\./ ) {  #no period, no extension at all, must be junk
            #print "EMPTY $d \n";
            next;
        } else {
            #print "\t" foreach (0..$depth+2);
            #print "SUB: $d\n";
            $content_count++;
        }
    }

    #print "\t" foreach (0..$depth+2);
    #print "$content_count files.\n";
    return $content_count;
}
sub _flush {
       my $h = select($_[0]); my $a=$|; $|=1; $|=$a; select($h);
}
sub _show_progress {
    my ($progress) = @_;
    my $stars   = '*' x int($progress*10);
    my $percent = sprintf '%.2f', ($progress*100);
    $percent = $percent >= 100.0 ? 'done.' : $percent.'%';
    print("\r$stars $percent");
    _flush(*STDOUT);
}
sub _casemove {
    my ($a,$b) = @_;
    my $r = sprintf '%4d', rand(9999);
    #warn "**** $a  => ".$a.'-'.$r;
    rmove($a, $a.'-'.$r) or die $!;
    #warn "**** ".$a.'-'.$r." $b";
    rmove($a.'-'.$r, $b) or die $!;
    return 1;
}
sub _md5sum{
    my $file = shift;
    my $digest = "";
    eval{
        open(FILE, $file) or die "Can't find file $file\n";
        my $ctx = Digest::MD5->new;
        $ctx->addfile(*FILE);
        $digest = $ctx->hexdigest;
        close(FILE);
    };
    if($@){
        print $@;
        return "";
    }
    return $digest;
}
sub _move_art {
    my ($ldir,$file,$reason) = @_;
    my ($base,$ext) = $file =~ m/(.+)\.(.{3})$/;
    next if $file eq 'folder.'.$ext; ## already matched how we want..
    print STDERR "MOVE ($reason) $ldir/$file -> $ldir/folder.$ext\n";
    move( "$ldir/$file", "$ldir/folder.$ext" ) or die "$ldir/$file $!";
    _convert_art("folder.$ext", "$ldir/folder.$ext", $ldir);
}
sub _convert_art {
    (local $_, my $name, my $dir) = @_;

    my ($ext) = m/^folder\.(jpg|png|bmp)$/i;
    return unless $ext;

    my $img = Image::Magick->new;
    $img->Read( "$name" ); # or die $!;
    my ($w,$h) = $img->Get('width','height');
    if ($w == 0 || $h == 0 ) {
        print STDERR "INVALID IMAGE: $name\n";
        return;
    }
    my $r = $w/$h;

    my $mod = 0;
    if ( $ext =~ /(png|bmp)/i && ! -e "$dir/folder.jpg") {
        #convert to jpg
        print STDERR "CONVERT IMAGE: $name\n";
        $mod = 1;
    }
   
    #resize first, crop later..
    
    if ( ($w-300) > 3 || ($h-300) > 3 ) {
        #resize
        print STDERR "RESIZE IMAGE: $name (${w}x${h})\n";
        $img->AdaptiveResize(height=>300);
        $mod = 1;
    } 
     
    my $c = ( $w > $h ? $h : $w ); #shortest side
    if ( abs($r - 1) > 0.011 && $c > 300 ) {
        #crop
        my ($offx, $offy) = int( ($w-$c)/2 ), int( ($h-$c)/2 );
        foreach ($offx, $offy) { $_ = $_ || 0 }
        print STDERR "CROP IMAGE: $name r: $r (${w}x$h) > (${c}x$c+$offx+$offy)\n";
        $img->Crop(geometry=>sprintf('%sx%s+%s+%s', $w, $h, $offx, $offy ));
        $mod = 1;
    }
    
    if ( /[A-Z]/ ) {  ## has uppercase
        #rename
        print STDERR "RENAME IMAGE: $name\n";
        $mod = 1;
    }
    
    if ( $mod ) {
        my @parts = split /\//, $name;
        shift @parts;
        move($name, $DIR->{BACKUP}.'/'.$parts[5].'-'.$parts[6].'-'.$_);
        $img->Set(quality => 85);
        $img->Write($dir.'/folder.jpg'); # or die $!;
    }
    
    ## else it's ok!!

    return 1;
}
sub _remove_extended_chars {
    my $a = shift;
    $a =~ s/[^\x00-\x7f]/_/g;
    return $a;
}
__END__
## PATCHED MP4::INFO to remove a lot of the assumtions made by the author.  Really noone would want to know album artist AND artist?
## dude you're writing a perl lib.. it's cool to have helpers but it's bullshit to obfuscate the frame data..
#
# Copyright (c) 2004-2010 Jonathan Harris <jhar@cpan.org>
#
# This program is free software; you can redistribute it and/or modify it
# under the the same terms as Perl itself.
#

package MP4::Info;

use overload;
use strict;
use Carp;
use Symbol;
use Encode;
use Encode::Guess qw(latin1);
use IO::String;

use vars qw(
	    $VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $AUTOLOAD
	    %data_atoms %other_atoms %container_atoms @mp4_genres
	   );

@ISA = 'Exporter';
@EXPORT      = qw(get_mp4tag get_mp4info);
@EXPORT_OK   = qw(use_mp4_utf8);
%EXPORT_TAGS = (
		utf8	=> [qw(use_mp4_utf8)],
		all	=> [@EXPORT, @EXPORT_OK]
	       );

$VERSION = '1.13';

my $debug = 0;


=head1 NAME

MP4::Info - Fetch info from MPEG-4 files (.mp4, .m4a, .m4p, .3gp)

=head1 SYNOPSIS

	#!perl -w
	use MP4::Info;
	my $file = 'Pearls_Before_Swine.m4a';

	my $tag = get_mp4tag($file) or die "No TAG info";
	printf "$file is a %s track\n", $tag->{GENRE};

	my $info = get_mp4info($file);
	printf "$file length is %d:%d\n", $info->{MM}, $info->{SS};

	my $mp4 = new MP4::Info $file;
	printf "$file length is %s, title is %s\n",
		$mp4->time, $mp4->title;

=head1 DESCRIPTION

The MP4::Info module can be used to extract tag and meta information from
MPEG-4 audio (AAC) and video files. It is designed as a drop-in replacement
for L<MP3::Info|MP3::Info>.

Note that this module does not allow you to update the information in MPEG-4
files.

=over 4

=item $mp4 = MP4::Info-E<gt>new(FILE)

OOP interface to the rest of the module. The same keys available via
C<get_mp4info> and C<get_mp4tag> are available via the returned object
(using upper case or lower case; but note that all-caps 'VERSION' will
return the module version, not the MPEG-4 version).

Passing a value to one of the methods will B<not> set the value for that tag
in the MPEG-4 file.

=cut

sub new
{
    my ($class, $file) = @_;

    # Supported tags
    my %tag_names =
	(
	 ALB => 1, APID => 1, ART => 1, CMT => 1, COVR => 1, CPIL => 1, CPRT => 1, DAY => 1, DISK => 1, GNRE => 1, GRP => 1, NAM => 1, RTNG => 1, TMPO => 1, TOO => 1, TRKN => 1, WRT => 1,
	 TITLE => 1, ARTIST => 1, ALBUM => 1, YEAR => 1, COMMENT => 1, GENRE => 1, TRACKNUM => 1,
	 VERSION => 1, LAYER => 1,
	 BITRATE => 1, FREQUENCY => 1, SIZE => 1,
	 SECS => 1, MM => 1, SS => 1, MS => 1, TIME => 1,
	 COPYRIGHT => 1, ENCODING => 1, ENCRYPTED => 1,
	);

    my $tags = get_mp4tag ($file) or return undef;
    my $self = {
		_permitted => \%tag_names,
		%$tags
	       };
    return bless $self, $class;
}


# Create accessor functions - see perltoot manpage
sub AUTOLOAD
{
    my $self = shift;
    my $type = ref($self) or croak "$self is not an object";
    my $name = $AUTOLOAD;
    $name =~ s/.*://;	# strip fully-qualified portion

    unless (exists $self->{_permitted}->{uc $name} )
    {
	croak "No method '$name' available in class $type";
    }

    # Ignore any parameter
    return $self->{uc $name};
}


sub DESTROY
{
}


############################################################################

=item use_mp4_utf8([STATUS])

Tells MP4::Info whether to assume that ambiguously encoded TAG info is UTF-8
or Latin-1. 1 is UTF-8, 0 is Latin-1. Default is UTF-8.

Function returns new status (1/0). If no argument is supplied, or an
unaccepted argument is supplied, function merely returns existing status.

This function is not exported by default, but may be exported
with the C<:utf8> or C<:all> export tag.

=cut

my $utf8 = 1;

sub use_mp4_utf8
{
    my ($val) = @_;
    $utf8 = $val if (($val == 0) || ($val == 1));
    return $utf8;
}


=item get_mp4tag (FILE)

Returns hash reference containing the tag information from the MP4 file.
The following keys may be defined:

	ALB	Album
	APID	Apple Store ID
	ART	Artist
	CMT	Comment
	COVR	Album art (typically JPEG or PNG data)
	CPIL	Compilation (boolean)
	CPRT	Copyright statement
	DAY	Year
	DISK	Disk number & total (2 integers)
	GNRE	Genre
	GRP	Grouping
	NAM	Title
	RTNG	Rating (integer)
	TMPO	Tempo (integer)
	TOO	Encoder
	TRKN	Track number & total (2 integers)
	WRT	Author or composer

For compatibility with L<MP3::Info|MP3::Info>, the MP3 ID3v1-style keys
TITLE, ARTIST, ALBUM, YEAR, COMMENT, GENRE and TRACKNUM are defined as
synonyms for NAM, ART, ALB, DAY, CMT, GNRE and TRKN[0].

Any and all of these keys may be undefined if the corresponding information
is missing from the MPEG-4 file.

On error, returns nothing and sets C<$@>.

=cut

sub get_mp4tag
{
    my ($file) = @_;
    my (%tags);

    return parse_file ($file, \%tags) ? undef : {%tags};
}


=item get_mp4info (FILE)

Returns hash reference containing file information from the MPEG-4 file.
The following keys may be defined:

	VERSION		MPEG version (=4)
	LAYER		MPEG layer description (=1 for compatibility with MP3::Info)
	BITRATE		bitrate in kbps (average for VBR files)
	FREQUENCY	frequency in kHz
	SIZE		bytes in audio stream

	SECS		total seconds, rounded to nearest second
	MM		minutes
	SS		leftover seconds
	MS		leftover milliseconds, rounded to nearest millisecond
	TIME		time in MM:SS, rounded to nearest second

	COPYRIGHT	boolean for audio is copyrighted
	ENCODING        audio codec name. Possible values include:
			'mp4a' - AAC, aacPlus
			'alac' - Apple lossless
			'drms' - Apple encrypted AAC
			'samr' - 3GPP narrow-band AMR
			'sawb' - 3GPP wide-band AMR
			'enca' - Unspecified encrypted audio
	ENCRYPTED	boolean for audio data is encrypted

Any and all of these keys may be undefined if the corresponding information
is missing from the MPEG-4 file.

On error, returns nothing and sets C<$@>.

=cut

sub get_mp4info
{
    my ($file) = @_;
    my (%tags);

    return parse_file ($file, \%tags) ? undef : {%tags};
}


############################################################################
# No user-servicable parts below


# Interesting atoms that contain data in standard format.
# The items marked ??? contain integers - I don't know what these are for
# but return them anyway because the user might know.
my %data_atoms =
    (
     AART => 1,	# Album artist - returned in ART field no ART found
     ALB  => 1,
     ART  => 1,
     CMT  => 1,
     COVR => 1, # Cover art
     CPIL => 1,
     CPRT => 1,
     DAY  => 1,
     DISK => 1,
     GEN  => 1,	# Custom genre - returned in GNRE field no GNRE found
     GNRE => 1,	# Standard ID3/WinAmp genre
     GRP  => 1,
     NAM  => 1,
     RTNG => 1,
     TMPO => 1,
     TOO  => 1,
     TRKN => 1,
     WRT  => 1,
     # Apple store
     APID => 1,
     AKID => 1,	# ???
     ATID => 1,	# ???
     CNID => 1,	# ???
     GEID => 1,	# Some kind of watermarking ???
     PLID => 1,	# ???
     # 3GPP
     TITL => 1,	# title       - returned in NAM field no NAM found
     DSCP => 1, # description - returned in CMT field no CMT found
     #CPRT=> 1,
     PERF => 1, # performer   - returned in ART field no ART found
     AUTH => 1,	# author      - returned in WRT field no WRT found
     #GNRE=> 1,
     MEAN => 1,
     NAME => 1,
     DATA => 1,
    );

# More interesting atoms, but with non-standard data layouts
my %other_atoms =
    (
     MOOV => \&parse_moov,
     MDAT => \&parse_mdat,
     META => \&parse_meta,
     MVHD => \&parse_mvhd,
     STSD => \&parse_stsd,
     UUID => \&parse_uuid,
    );

# Standard container atoms that contain either kind of above atoms
my %container_atoms =
    (
     ILST => 1,
     MDIA => 1,
     MINF => 1,
     STBL => 1,
     TRAK => 1,
     UDTA => 1,
     '----' => 1,	# iTunes and aacgain info
    );


# Standard ID3 plus non-standard WinAmp genres
my @mp4_genres =
    (
     'N/A', 'Blues', 'Classic Rock', 'Country', 'Dance', 'Disco',
     'Funk', 'Grunge', 'Hip-Hop', 'Jazz', 'Metal', 'New Age', 'Oldies',
     'Other', 'Pop', 'R&B', 'Rap', 'Reggae', 'Rock', 'Techno',
     'Industrial', 'Alternative', 'Ska', 'Death Metal', 'Pranks',
     'Soundtrack', 'Euro-Techno', 'Ambient', 'Trip-Hop', 'Vocal',
     'Jazz+Funk', 'Fusion', 'Trance', 'Classical', 'Instrumental',
     'Acid', 'House', 'Game', 'Sound Clip', 'Gospel', 'Noise',
     'AlternRock', 'Bass', 'Soul', 'Punk', 'Space', 'Meditative',
     'Instrumental Pop', 'Instrumental Rock', 'Ethnic', 'Gothic',
     'Darkwave', 'Techno-Industrial', 'Electronic', 'Pop-Folk',
     'Eurodance', 'Dream', 'Southern Rock', 'Comedy', 'Cult', 'Gangsta',
     'Top 40', 'Christian Rap', 'Pop/Funk', 'Jungle', 'Native American',
     'Cabaret', 'New Wave', 'Psychadelic', 'Rave', 'Showtunes',
     'Trailer', 'Lo-Fi', 'Tribal', 'Acid Punk', 'Acid Jazz', 'Polka',
     'Retro', 'Musical', 'Rock & Roll', 'Hard Rock', 'Folk',
     'Folk/Rock', 'National Folk', 'Swing', 'Fast-Fusion', 'Bebob',
     'Latin', 'Revival', 'Celtic', 'Bluegrass', 'Avantgarde',
     'Gothic Rock', 'Progressive Rock', 'Psychedelic Rock',
     'Symphonic Rock', 'Slow Rock', 'Big Band', 'Chorus',
     'Easy Listening', 'Acoustic', 'Humour', 'Speech', 'Chanson',
     'Opera', 'Chamber Music', 'Sonata', 'Symphony', 'Booty Bass',
     'Primus', 'Porn Groove', 'Satire', 'Slow Jam', 'Club', 'Tango',
     'Samba', 'Folklore', 'Ballad', 'Power Ballad', 'Rhythmic Soul',
     'Freestyle', 'Duet', 'Punk Rock', 'Drum Solo', 'A capella',
     'Euro-House', 'Dance Hall', 'Goa', 'Drum & Bass', 'Club House',
     'Hardcore', 'Terror', 'Indie', 'BritPop', 'NegerPunk',
     'Polsk Punk', 'Beat', 'Christian Gangsta', 'Heavy Metal',
     'Black Metal', 'Crossover', 'Contemporary C', 'Christian Rock',
     'Merengue', 'Salsa', 'Thrash Metal', 'Anime', 'JPop', 'SynthPop'
    );


sub parse_file
{
    my ($file, $tags) = @_;
    my ($fh, $err, $header, $size);

    if (not (defined $file && $file ne ''))
    {
	$@ = 'No file specified';
	return -1;
    }

    if (ref $file)	# filehandle passed
    {
	$fh = $file;
    }
    else
    {
	$fh = gensym;
	if (not open $fh, "< $file\0")
	{
	    $@ = "Can't open $file: $!";
	    return -1;
	}
    }

    binmode $fh;

    # Sanity check that this looks vaguely like an MP4 file
    if ((read ($fh, $header, 8) != 8) || (lc substr ($header, 4) ne 'ftyp'))
    {
	close ($fh);
	$@ = 'Not an MPEG-4 file';
	return -1;
    }
    seek $fh, 0, 2;
    $size = tell $fh;
    seek $fh, 0, 0;

    $err = parse_container($fh, 0, $size, $tags);
    close ($fh);
    return $err if $err;

    # remaining get_mp4tag() stuff
    $tags->{CPIL}     = 0                unless defined ($tags->{CPIL});

    # MP3::Info compatibility
    $tags->{TITLE}    = $tags->{NAM}     if defined ($tags->{NAM});
    $tags->{ARTIST}   = $tags->{ART}     if defined ($tags->{ART});
    $tags->{ALBUM}    = $tags->{ALB}     if defined ($tags->{ALB});
    $tags->{YEAR}     = $tags->{DAY}     if defined ($tags->{DAY});
    $tags->{COMMENT}  = $tags->{CMT}     if defined ($tags->{CMT});
    $tags->{GENRE}    = $tags->{GNRE}    if defined ($tags->{GNRE});
    $tags->{TRACKNUM} = $tags->{TRKN}[0] if defined ($tags->{TRKN});

    # remaining get_mp4info() stuff
    $tags->{VERSION}  = 4;
    $tags->{LAYER}    = 1                if defined ($tags->{FREQUENCY});
    $tags->{COPYRIGHT}= (defined ($tags->{CPRT}) ? 1 : 0);
    $tags->{ENCRYPTED}= 0                unless defined ($tags->{ENCRYPTED});

    # Returns actual (not requested) bitrate
    if (defined($tags->{SIZE}) && $tags->{SIZE} && defined($tags->{SECS}) && ($tags->{MM}+$tags->{SS}+$tags->{MS}))
    {
	$tags->{BITRATE}  = int (0.5 + $tags->{SIZE} * 0.008 / ($tags->{MM}*60+$tags->{SS}+$tags->{MS}/1000))
    }

    # Post process '---' container
    if ($tags->{MEAN} && ref($tags->{MEAN}) eq 'ARRAY')
    {
	for (my $i = 0; $i < scalar @{$tags->{MEAN}}; $i++)
	{
	    push @{$tags->{META}}, {
				    MEAN => $tags->{MEAN}->[$i],
				    NAME => $tags->{NAME}->[$i],
				    DATA => $tags->{DATA}->[$i],
				   };
	}

	delete $tags->{MEAN};
	delete $tags->{NAME};
	delete $tags->{DATA};
    }

    return 0;
}


# Pre:	$size=size of container contents
#	$fh points to start of container contents
# Post:	$fh points past end of container contents
sub parse_container
{
    my ($fh, $level, $size, $tags) = @_;
    my ($end, $err);

    $level++;
    $end = (tell $fh) + $size;
    while (tell $fh < $end)
    {
	$err = parse_atom($fh, $level, $end-(tell $fh), $tags);
	return $err if $err;
    }
    if (tell $fh != $end)
    {
	$@ = 'Parse error';
	return -1;
    }
    return 0;
}


# Pre:	$fh points to start of atom
#	$parentsize is remaining size of parent container
# Post:	$fh points past end of atom
sub parse_atom
{
    my ($fh, $level, $parentsize, $tags) = @_;
    my ($header, $size, $id, $err, $pos);
    if (read ($fh, $header, 8) != 8)
    {
	$@ = 'Premature eof';
	return -1;
    }

    ($size,$id) = unpack 'Na4', $header;
    if ($size==0)
    {
	# Zero-sized atom extends to eof (14496-12:2004 S4.2)
	$pos=tell($fh);
	seek $fh, 0, 2;
	$size = tell($fh) - $pos;	# Error if parent size doesn't match
	seek $fh, $pos, 0;
    }
    elsif ($size == 1)
    {
	# extended size
	my ($hi, $lo);
	if (read ($fh, $header, 8) != 8)
	{
	    $@ = 'Premature eof';
	    return -1;
	}
	($hi,$lo) = unpack 'NN', $header;
	$size=$hi*(2**32) + $lo;
	if ($size>$parentsize)
	{
	    # atom extends outside of parent container - skip to end of parent
	    seek $fh, $parentsize-16, 1;
	    return 0;
	}
	$size -= 16;
    }
    else
    {
	if ($size>$parentsize)
	{
	    # atom extends outside of parent container - skip to end of parent
	    seek $fh, $parentsize-8, 1;
	    return 0;
	}
	$size -= 8;
    }
    if ($size<0)
    {
	$@ = 'Parse error';
	return -1;
    }
    $id =~ s/[^\w\-]//;
    $id = uc $id;

    printf "%s%s: %d bytes\n", ' 'x(2*$level), $id, $size if $debug;

    if (defined($data_atoms{$id}))
    {
	return parse_data ($fh, $level, $size, $id, $tags);
    }
    elsif (defined($other_atoms{$id}))
    {
	return &{$other_atoms{$id}}($fh, $level, $size, $tags);
    }
    elsif ($container_atoms{$id})
    {
	return parse_container ($fh, $level, $size, $tags);
    }

    # Unkown atom - skip past it
    seek $fh, $size, 1;
    return 0;
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_moov
{
    my ($fh, $level, $size, $tags) = @_;

    # MOOV is a normal container.
    # Read ahead to improve performance on high-latency filesystems.
    my $data;
    if (read ($fh, $data, $size) != $size)
    {
	$@ = 'Premature eof';
	return -1;
    }
    my $cache=IO::String->new($data);
    return parse_container ($cache, $level, $size, $tags);
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_mdat
{
    my ($fh, $level, $size, $tags) = @_;

    $tags->{SIZE} = 0 unless defined($tags->{SIZE});
    $tags->{SIZE} += $size;
    seek $fh, $size, 1;

    return 0;
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_meta
{
    my ($fh, $level, $size, $tags) = @_;

    # META is just a container preceded by a version field
    seek $fh, 4, 1;
    return parse_container ($fh, $level, $size-4, $tags);
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_mvhd
{
    my ($fh, $level, $size, $tags) = @_;
    my ($data, $version, $scale, $duration, $secs);

    if ($size < 32)
    {
	$@ = 'Parse error';
	return -1;
    }
    if (read ($fh, $data, $size) != $size)
    {
	$@ = 'Premature eof';
	return -1;
    }

    $version = unpack('C', $data) & 255;
    if ($version==0)
    {
	($scale,$duration) = unpack 'NN', substr ($data, 12, 8);
    }
    elsif ($version==1)
    {
	my ($hi,$lo);
	print "Long version\n" if $debug;
	($scale,$hi,$lo) = unpack 'NNN', substr ($data, 20, 12);
	$duration=$hi*(2**32) + $lo;
    }
    else
    {
	return 0;
    }

    printf "  %sDur/Scl=$duration/$scale\n", ' 'x(2*$level) if $debug;
    $secs=$duration/$scale;
    $tags->{SECS} = int (0.5+$secs);
    $tags->{MM}   = int ($secs/60);
    $tags->{SS}   = int ($secs - $tags->{MM}*60);
    $tags->{MS}   = int (0.5 + 1000*($secs - int ($secs)));
    $tags->{TIME} = sprintf "%02d:%02d",
	$tags->{MM}, $tags->{SECS} - $tags->{MM}*60;

    return 0;
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_stsd
{
    my ($fh, $level, $size, $tags) = @_;
    my ($data, $data_format);

    if ($size < 44)
    {
	$@ = 'Parse error';
	return -1;
    }
    if (read ($fh, $data, $size) != $size)
    {
	$@ = 'Premature eof';
	return -1;
    }

    # Assumes first entry in table contains the data
    printf "  %sSample=%s\n", ' 'x(2*$level), substr ($data, 12, 4) if $debug;
    $data_format = lc substr ($data, 12, 4);

    # Is this an audio track? (Ought to look for presence of an SMHD uncle
    # atom instead to allow for other audio data formats).
    if (($data_format eq 'mp4a') ||	# AAC, aacPlus
	($data_format eq 'alac') ||	# Apple lossless
	($data_format eq 'drms') ||	# Apple encrypted AAC
	($data_format eq 'samr') ||	# Narrow-band AMR
	($data_format eq 'sawb') ||	# AMR wide-band
	($data_format eq 'sawp') ||	# AMR wide-band +
	($data_format eq 'enca'))	# Generic encrypted audio
    {
	$tags->{ENCODING} = $data_format;
#	$version = unpack "n", substr ($data, 24, 2);
#       s8.16 is inconsistent. In practice, channels always appears == 2.
#	$tags->{STEREO}  = (unpack ("n", substr ($data, 32, 2))  >  1) ? 1 : 0;
#       Old Quicktime field. No longer used.
#	$tags->{VBR}     = (unpack ("n", substr ($data, 36, 2)) == -2) ? 1 : 0;
	$tags->{FREQUENCY} = unpack ('N', substr ($data, 40, 4)) / 65536000;
	printf "  %sFreq=%s\n", ' 'x(2*$level), $tags->{FREQUENCY} if $debug;
    }

    $tags->{ENCRYPTED}=1 if (($data_format eq 'drms') ||
			     (substr($data_format, 0, 3) eq 'enc'));

    return 0;
}


# User-defined box. Used by PSP - See ffmpeg libavformat/movenc.c
#
# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_uuid
{
    my ($fh, $level, $size, $tags) = @_;
    my $data;

    if (read ($fh, $data, $size) != $size)
    {
	$@ = 'Premature eof';
	return -1;
    }
    ($size > 26) || return 0;	# 16byte uuid, 10byte psp-specific

    my ($u1,$u2,$u3,$u4)=unpack 'a4NNN', $data;
    if ($u1 eq 'USMT')	#  PSP also uses a uuid starting with 'PROF'
    {
	my ($pspsize,$pspid) = unpack 'Na4', substr ($data, 16, 8);
	printf "  %s$pspid: $pspsize bytes\n", ' 'x(2*$level) if $debug;
	($pspsize==$size-16) || return 0;	# sanity check
	if ($pspid eq 'MTDT')
	{
	    my $nblocks = unpack 'n', substr ($data, 24, 2);
	    $data = substr($data, 26);
	    while ($nblocks)
	    {
		my ($bsize, $btype, $flags, $ptype) = unpack 'nNnn', $data;
		printf "    %s0x%x: $bsize bytes, Type=$ptype\n", ' 'x(2*$level), $btype if $debug;
		if ($btype==1 && $bsize>12 && $ptype==1 && !defined($tags->{NAM}))
		{
		    # Could have titles in different langauges - use first
		    $tags->{NAM} = decode("UTF-16BE", substr($data, 10, $bsize-12));
		}
		elsif ($btype==4 && $bsize>12 && $ptype==1)
		{
		    $tags->{TOO} = decode("UTF-16BE", substr($data, 10, $bsize-12));
		}
		$data = substr($data, $bsize);
		$nblocks-=1;
	    }
	}
    }
    return 0;
}


# Pre:	$size=size of atom contents
#	$fh points to start of atom contents
# Post:	$fh points past end of atom contents
sub parse_data
{
    my ($fh, $level, $size, $id, $tags) = @_;
    my ($data, $atom, $type);

    if (read ($fh, $data, $size) != $size)
    {
	$@ = 'Premature eof';
	return -1;
    }

    # 3GPP - different format when child of 'udta'.
    # Let existing tags (if any) override these.
    # WHY ARE YOU DOING THIS, JUST GIVE ME THE GODDAMNED TAGS
    if (($id eq 'TITL'))
    {
	my ($ver) = unpack 'N', $data;
	if ($ver == 0)
	{
	    ($size > 7) || return 0;
	    $size -= 7;
	    $type = 1;
	    $data = substr ($data, 6, $size);

	    if ($id eq 'TITL')
	    {
		return 0 if defined ($tags->{NAM});
		$id = 'NAM';
	    }
	}
    }

    # Parse out the tuple that contains aacgain data, etc.
    if (($id eq 'MEAN') ||
	($id eq 'NAME') ||
	($id eq 'DATA'))
    {
	# The first 4 or 8 bytes are nulls.
	if ($id eq 'DATA')
	{
	    $data = substr ($data, 8);
	}
	else
	{
	    $data = substr ($data, 4);
	}

	push @{$tags->{$id}}, $data;
	return 0;
    }

    if (!defined($type))
    {
	($size > 16) || return 0;

	# Assumes first atom is the data atom we're after
	($size,$atom,$type) = unpack 'Na4N', $data;
	(lc $atom eq 'data') || return 0;
	($size > 16) || return 0;
	$size -= 16;
	$type &= 255;
	$data = substr ($data, 16, $size);
    }
    printf "  %sType=$type, Size=$size\n", ' 'x(2*$level) if $debug;

    if ($id eq 'COVR')
    {
	# iTunes appears to use random data types for cover art
	$tags->{$id} = $data;
    }
    elsif ($type==0)	# 16bit int data array
    {
	my @ints = unpack 'n' x ($size / 2), $data;
	if ($id eq 'GNRE')
	{
	    $tags->{$id} = $mp4_genres[$ints[0]];
	}
	elsif ($id eq 'DISK' or $id eq 'TRKN')
	{
	    # Real 10.0 sometimes omits the second integer, but we require it
	    $tags->{$id} = [$ints[1], ($size>=6 ? $ints[2] : 0)] if ($size>=4);
	}
	elsif ($size>=4)
	{
	    $tags->{$id} = $ints[1];
	}
    }
    elsif ($type==1)	# Char data
    {
	# faac 1.24 and Real 10.0 encode data as unspecified 8 bit, which
	# goes against s8.28 of ISO/IEC 14496-12:2004. How tedious.
	# Assume data is utf8 if it could be utf8, otherwise assume latin1.
	my $decoder = Encode::Guess->guess ($data);
	$data = (ref ($decoder)) ?
	    $decoder->decode($data) :	# found one of utf8, utf16, latin1
	    decode($utf8 ? 'utf8' : 'latin1', $data);	# ambiguous so force

	if ($id eq 'GEN')
	{
	    return 0 if defined ($tags->{GNRE});
	    $id='GNRE';
	}
	elsif ($id eq 'DAY')
	{
	    $data = substr ($data, 0, 4);
	    # Real 10.0 supplies DAY=0 instead of deleting the atom if the
	    # year is not known. What's wrong with these people?
	    return 0 if $data==0;
	}
	$tags->{$id} = $data;
    }
    elsif ($type==21)	# Integer data
    {
	# Convert to an integer if of an appropriate size
	if ($size==1)
	{
	    $tags->{$id} = unpack 'C', $data;
	}
	elsif ($size==2)
	{
	    $tags->{$id} = unpack 'n', $data;
	}
	elsif ($size==4)
	{
	    $tags->{$id} = unpack 'N', $data;
	}
	elsif ($size==8)
	{
	    my ($hi,$lo);
	    ($hi,$lo) = unpack 'NN', $data;
	    $tags->{$id} = $hi*(2**32) + $lo;
	}
	else
	{
	    # Non-standard size - just return the raw data
	    $tags->{$id} = $data;
	}
    }

    # Silently ignore other data types
    return 0;
}

1;
