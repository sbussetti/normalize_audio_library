#!/usr/bin/perl
use strict;
use MP3::Tag;
use Text::Capitalize qw/capitalize_title/;
#use Data::Dumper;
use File::Find;
use File::Copy qw/copy move/;
use Digest::MD5 qw/md5_hex/;
use File::Copy::Recursive qw/rcopy rmove dirmove/;
use File::Path qw/make_path remove_tree/;
use File::Basename;
use DB_File;
use Storable qw/freeze thaw/;
use Getopt::Long;
use Image::Magick;
use File::Spec; 

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
#
# Testing:
# * Another duplication/validation strategy would be to count up all ocurrances of track/disc numbers for albums that match on title.
#   if done as a prostproc, we'd already know the directories would be in place and album names corrected.  
# Done:
# * Albums with same name but varying capitalization.  Should be fixed before casemove 
#   operations or else you end up with an endless rename cycle on subsequent runs.
############################
my $DIR = {};
my $BACKUP_DIR = '/cygdrive/e/Unsorted/_tmp/audio-script-backup';
my $ROOT_DIR = my $SEARCH_DIR = '/cygdrive/d/Audio/Libraries/Original Library';
my $EMPTY_DIR =  '/cygdrive/e/Unsorted/_tmp/audio-empty-folders';
my $FILE_COUNT = 0;
my $TOTAL_FILES = 0;


MP3::Tag->config(write_v24 => 'TRUE'); ## enable "mostly acceptable" ID3v2.4 writing

my $z = tie my %FILE_HASH, 'DB_File', '/tmp/file_hash.db', O_RDWR|O_CREAT, 0666, $DB_HASH
        or die "cannot open file: $!\n";

sub cleanup {
    print "\nExiting... Clean up\n";
    foreach my $HH ( $z ) {
        $HH->sync && untie $HH;
    }
}

$SIG{INT} = sub { exit };
END { &cleanup }

GetOptions(
    'total_files|t=i' => \$TOTAL_FILES,  ## in case you know and don't want to wait for a scan
    'search_dir|s=s' => \$SEARCH_DIR,   ## where your files to be organized live
    'root_dir|r=s' => \$ROOT_DIR,     ## where you want organized files to end up.  If not specified, fix is done in-place
    'backup_dir|b=s' => \$BACKUP_DIR, ## where you want backups to go of modified files..

    'locate_duplicates|d' => \&dispatch,  ## NOT VERY SMART YET
    'list_duplicates|l' => \&dispatch,    
    'normalize_paths|n' => \&dispatch,
    'locate_lowres_images|i' => \&dispatch,  ## STILL UNDER DEVELOPMENT...
    'help|h' => \&help,
);

sub dispatch {
    my $opt = (shift);
    $DIR = {
        ROOT => File::Spec->canonpath($ROOT_DIR),
        SEARCH => File::Spec->canonpath($SEARCH_DIR),
        EMPTY => File::Spec->canonpath($EMPTY_DIR),
        BACKUP => File::Spec->canonpath($BACKUP_DIR),
    };

    ## ensure releif dirs exist
    foreach my $d ($EMPTY_DIR, $BACKUP_DIR) {
        make_path($d) unless -d $d;
    }
    
    #print Dumper($DIR), "\n";
    my $map = {
        'locate_duplicates' => \&locate_duplicates,
        'list_duplicates' => \&list_duplicates,
        'normalize_paths' => \&normalize_paths,
        'locate_lowres_images' => \&locate_lowres_images
    };
    $map->{$opt}->();
}
sub help {
    print q[
normalize_audio_library.pl 

I am using a crummy old options library and arguments are processed positionally.
Directory options must all preceed processing options.
Usage: normalize_audio_library.pl [DIRECTORY OPTIONS] [PROCESSING OPTIONS]

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
            find({wanted => \&wanted__filecount_mp3_finder}, $DIR->{SEARCH});
        }    
    }
    print "\nFound $TOTAL_FILES to process.  beginning search...\n";
}
sub wanted__filecount_mp3_finder { 
        next unless -f && /^(.*\.mp3)$/i; $TOTAL_FILES++; print "\rFound: $TOTAL_FILES"; _flush(*STDOUT); 
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
    next unless -f && /^(.*\.mp3)$/i; 

    $FILE_COUNT++;
    _show_progress($FILE_COUNT/$TOTAL_FILES);
    my $md5; 
    my $hkey = uc($File::Find::name);
    if ( ! exists $FILE_HASH{$hkey} ) {
        $md5 = _md5sum($File::Find::name);
        print STDERR "NEW: $hkey ($md5)\n";
        if ( ! $md5 ) {
            print STDERR "NO HEX DIGEST OF $File::Find::name\n";
            return;
        }
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
        my $mp3 = MP3::Tag->new("$File::Find::name");
        $mp3->update_tags({Conductor => 'BAD ART'});
        $mp3->update_tags;
        print STDERR "$qual: $File::Find::name )) $format: Depth: $depth, Density: $density, Quality: $quality \n";
    }
}

my @LEAVE_DIRS;
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
            wanted => \&wanted__fix_paths,
            postprocess => \&postprocess__normalize
        }, @CURRENT_JOB);
    }
}
sub wanted__fix_paths {

    ## moves audio files only to paths following their album / artist / track names
    ## ignores all other files, except folder.jpg...
    my ($name, $ext) = m/^(.*)\.(mp3)$/i;
    next unless $ext and -f $File::Find::name;

    $FILE_COUNT++;
    _show_progress($FILE_COUNT/$TOTAL_FILES);

    my ($mp3, $id3) = _get_tags($File::Find::name);
    print STDERR "ID3 NOT AVAILABLE FOR: $File::Find::name\n" and next unless $id3;
    my $tag = {};

    eval{ $tag = $mp3->autoinfo(); };
    print STDERR "\nBAD TAG: $File::Find::name\n" && next if $@;
    $tag->{'disk'} = $mp3->disk1;
    $tag->{'album_artist'} = $id3->get_frame('TPE2');

    foreach ( keys %$tag) {
        $tag->{$_} =~ s/[^\x00-\x7f]/_/g;
        $tag->{$_} =~ s/([\/\\\*\|\:"\<\>\?]|\.$)/_/g;
        print STDERR "ERROR: Field $_ is blank\n" && return if $_ ne 'disk' && /^\s*$/;
    }

    my $md5;
    if ( ! exists $FILE_HASH{uc($File::Find::name)} ) {
        $md5 = _md5sum($File::Find::name);
        if ( ! $md5 ) {
            print STDERR "NO HEX DIGEST OF $File::Find::name\n";
            return;
        }
        $FILE_HASH{uc($File::Find::name)} = $md5;
    } else {
        $md5 = $FILE_HASH{uc($File::Find::name)};
    }    

    my @pparts = ( ( $tag->{album_artist} || $tag->{artist} ), $tag->{album} );
    my $pkey = join '/', @pparts; ##no disk...
    if ( $tag->{'disk'} ) {
        push @pparts, sprintf('Disc %s', $tag->{'disk'});
    }
    my $new_relpath = join '/', @pparts;
    my $new_basename = sprintf '%s - %02d - %s.mp3', $tag->{artist}, $tag->{track}, $tag->{title};

    ##directory prep...
    my $full_path = File::Spec->catpath(undef, $DIR->{ROOT}, $new_relpath);
    if ( -d $full_path and 
        uc($File::Find::dir) eq uc($full_path) and
        $File::Find::dir ne $full_path
        ) { ## case issues -- really here we should be tracking an rebuilding @_ ... 
            my @oldparts = split /\//, $File::Find::dir;
            my @newparts = split /\//, $full_path;

            #for ( my $i = $#oldparts ; $i >= 0; $i-- ) {
            while (my ($a, $b) = (pop @oldparts, pop @newparts)) {    
                if ( $a ne $b ) {  ## again cap error..
                    print STDERR "MOVE PATH: ".join('/',@oldparts)." => ".join('/',@newparts)."\n";
                    my $invalid = join('/',@oldparts);
                    _casemove($invalid, join('/',@newparts));

                    push @LEAVE_DIRS, $invalid; ## it's messed up, up to the highest level one we had to move...

                    print STDERR "LEAVE DIR: $File::Find::dir >> $invalid\n";
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
    if ( $full_pathname eq $File::Find::name ) {
        #no action..., same path
        return;

    } elsif ( -e $full_pathname && ! uc($full_pathname) eq uc($File::Find::name ) ) {
        ## same path.  if it's a capitalization difference, move.. since exacts already bailed..

        #unless of course, the target file which already exists is actually the same file..
        my $target_md5;
        if ( ! exists $FILE_HASH{uc($full_pathname)} ) {
            $target_md5 = _md5sum($full_pathname);
            if ( $target_md5 ) {
                $FILE_HASH{uc($full_pathname)} = $target_md5;
            }
        } else {
            $target_md5 = $FILE_HASH{uc($full_pathname)};
        }

        ##done caching, do the compare here..  I guess if the target exists but we can't get a sum we overwrite it..
        if ( $target_md5 && $md5 ne $target_md5 ) {  ## not the same file after all, so skip for now..
            print STDERR "\nFILE EXISTS: $File::Find::name => $full_pathname\n skipping...\n";
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


    #my $nodisc_root = $File::Find::dir;
    #$nodisc_root =~ s/\/?\(?\s*(CD|Dis[kc])\s*(\d|[a-z])\s*\)?$//i;



    print STDERR "MOVE: $File::Find::name => $full_pathname\n";


    if ( uc($File::Find::name) eq uc($full_pathname) ) {
        _casemove( $File::Find::name, $full_pathname ) or die "Failed to move: $File::Find::name => $full_pathname";
    } else {
        move( $File::Find::name, $full_pathname ) or die "Failed to move: $File::Find::name => $full_pathname";
    }

    delete $FILE_HASH{uc($File::Find::name)};
    $FILE_HASH{uc($full_pathname)} = $md5;

    if ( -e $File::Find::dir.'/folder.jpg' && "$File::Find::dir" ne $full_path ) {
        move ( "$File::Find::dir/folder.jpg", "$full_path/folder.jpg" ) or die $@;
        print STDERR "MOVE ART: $File::Find::dir/folder.jpg => $full_path/folder.jpg\n";
    }

    
}

my $DIRALBTRACKS;
sub postprocess__normalize {
    $DIRALBTRACKS = {};
    @_ = _postprocess__count_album_tracks(@_);
    @_ = _postprocess__remove_empty_folders(@_);
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
}
sub _postprocess__remove_empty_folders {
    #warn "TEST: $File::Find::dir";
    if ( $File::Find::dir ne $DIR->{SEARCH} && ! _recurse_for_empty($File::Find::dir) ) {
        ## turtles all the way down
        #warn "Removing empty: $File::Find::dir";
        #remove_tree($File::Find::dir)
        my $e = basename($File::Find::dir);

        my $tp = $DIR->{EMPTY}.'/'.$e ;
        my $md = 0;
        my $loop = 1;
        do {
            if ( -e $tp.($md ? '.'.$md : '') ) {
                $md++;
            } else {
                $loop = 0;
            }
        } while ($loop);
        print STDERR "Removing empty dir: $File::Find::dir\n";
        my $emptyd = $tp.($md ? '.'.$md : ''); 
        _casemove( $File::Find::dir, $emptyd) or die "Failed to move: $File::Find::name => $emptyd";
    }

    return @_;
}
sub _postprocess__count_album_tracks {
    foreach my $f ( @_ ) {
        my $lf = $File::Find::dir.'/'.$f;
        next if $f =~ /^\.{1,2}$/ || ! -f $lf || $f !~ /\.mp3$/i;
        
        #my $mp3 = MP3::Tag->new("$lf");
        #$mp3->get_tags();
        #my $id3 = $mp3->{ID3v2};

        my ($mp3, $id3) = _get_tags($lf);

        print STDERR "ID3 NOT AVAILABLE FOR: $lf\n" and next unless $id3;

        my $tag = {};

        eval{ $tag = $mp3->autoinfo(); };
        print STDERR "\nBAD TAG: $lf\n" and next if $@;
        $tag->{'disk'} = $mp3->disk1;
        $tag->{'album_artist'} = $id3->get_frame('TPE2');

        my $disk = ( $tag->{'disk'} || 1 );
        my $artist = ( $tag->{'album_artist'} || $tag->{'artist'} );
        my $track = int $tag->{'track'};
        $DIRALBTRACKS->{$artist}->{$tag->{'album'}}->{$disk}->{$track}++;
    }
    return @_;
}

sub preprocess__normalize {
    @_ = _preprocess__fix_album_artist_capitalization(@_);
    @_ = _preprocess__find_album_art(@_);
}
sub _preprocess__fix_album_artist_capitalization {
    foreach my $f ( @_ ) {
        my $lf = $File::Find::dir.'/'.$f;
        next if $f =~ /^\.{1,2}$/ || ! -f $lf || $f !~ /\.mp3$/i;
        
        #my $mp3 = MP3::Tag->new("$lf");
        #$mp3->get_tags();
        #my $id3 = $mp3->{ID3v2};

        my ($mp3, $id3) = _get_tags($lf);

        print STDERR "ID3 NOT AVAILABLE FOR: $lf\n" and next unless $id3;

        my $tag = {};

        eval{ $tag = $mp3->autoinfo(); };
        print STDERR "\nBAD TAG: $lf\n" and next if $@;
        $tag->{'disk'} = $mp3->disk1;
        $tag->{'album_artist'} = $id3->get_frame('TPE2');

        my $update = {};
        if ( ! defined $tag->{'album_artist'} || $tag->{'album_artist'} =~ /^\s*$/ ) {
            print STDERR "Filling in album artist for $lf\n";
            $update->{'album_artist'} = $tag->{'album_artist'} = $tag->{'artist'};
        }


        my $fix = {
            album => capitalize_title($tag->{album}, PRESERVE_ALLCAPS => 1),
            album_artist => capitalize_title($tag->{album_artist}, PRESERVE_ALLCAPS => 1),
            artist => capitalize_title($tag->{artist}, PRESERVE_ALLCAPS => 1)
        };
        foreach my $k (qw/artist album_artist/) { 
            $fix->{$k} =~ s/(.+),\s+The\s*$/The $1/i;
        }

        foreach my $k ( keys %$fix ) {
            if ($tag->{$k} ne $fix->{$k}) {
                $update->{$k} = $fix->{$k};
            }
        }

        if (my @uk = keys %$update) {
            my @pairs = map { "([$_] $update->{$_})" } @uk;
            print STDERR "Updating tags ".join(' ',@pairs)."\n";
            
            if ($update->{'album_artist'}) {
                $mp3->set_id3v2_frame('TPE2', $update->{'album_artist'});
                delete $update->{'album_artist'};
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



sub _get_tags {
    my $file = shift;
    my $mp3 = MP3::Tag->new($file) or die "Cannot parse Tags for: $file";
    $mp3->get_tags();
    my $id3 = $mp3->{ID3v2};

    unless ($id3) {

        my $id31 = $mp3->{ID3v1};
        print STDERR "ID3v2 and 1 NOT AVAILABLE FOR: $File::Find::name\n" and return () unless $id31;
        my $extract = {};
        for my $t (qw/artist album year comment track genre/) {
            my $tag = $id31->$t;
            $extract->{$t} = $tag if defined $tag and $tag ne '';
        }

        
        $id3 = $mp3->new_tag('ID3v2');
        $mp3->update_tags($extract);
        $mp3->update_tags;
    }

    return ($mp3, $id3);
}
sub _recurse_for_empty {
    my ($ddir, $depth) = (shift, shift);
    my $base = basename($ddir); 
    #print   $depth;
    #print "\t" foreach (0..$depth+1);
    #print        "$base contains:\n";

    opendir my $RDIR, $ddir or die "$ddir >> $!";
    my @check_dirs = grep { ! /^\.{1,2}$/ } readdir $RDIR;
    closedir $RDIR;


    my $content_count = 0;
    foreach my $d ( @check_dirs ) {
        if ( -d $ddir.'/'.$d ) {
            $content_count += _recurse_for_empty($ddir.'/'.$d, $depth+1);
        } elsif ( $d =~ /\.(cue|sfv|doc|nfo|log|m3u|ini|db|jpe?g|png|bmp|txt)$/i || $d !~ /\./ ) {  #no period, no extension at all, must be junk
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



__END__
sub normalize_album_folders { ## find empties...
    my @parts = split /\//, $File::Find::dir;
    my $cleandir = $File::Find::dir;  #to be cleaned..

    shift @parts;  ## clear initial empty index for leading /
    if ( @parts == 7 ) {
        #print "ALBUM: $File::Find::dir\n";
    } elsif ( @parts > 7 && ! ( @parts == 8 && $parts[7] =~ /^(CD|Disc|Video) ?[A-Z1-9]+$/i ) ) {
        print "NESTED: $File::Find::dir\n";
        if ( $parts[5] eq $parts[6] ) {  ## repeat album..
            splice @parts, 6, 1;
            $cleandir = '/'.join '/', @parts;        
        }

    } 
    ## check for invalid characters on all folders..
PARTLOOP:    foreach my $part (@parts) {    
        if ( $part !~ /^[A-Z0-9\^\&\'\@\{\}\[\],\$\=\!\-\#\(\)\%\.\+\~_ ]+$/i || $part =~ /\.$/ ) {
            #print "INVALID? ($part) $File::Find::dir\n";
            $cleandir =~ s/(["\?]|\.$)/_/g;
            last PARTLOOP;
        }
    }

    if ( $File::Find::dir ne $cleandir ) {
        print "WILL MOVE: $File::Find::dir -> $cleandir\n";
        rmove ($File::Find::dir,$cleandir) or die $!;
    }

    ##open each dir and check if empty..
    opendir DDIR, $cleandir;
    my $childcount = 0;
    while ( local $_ = readdir DDIR ) {
        next if /^\.{1,2}$/;
        $childcount++;
    }
    closedir DDDIR;
    if ( ! $childcount ) {
        print "EMPTY: (removing) $cleandir\n";
        rmdir $cleandir or die $!;
    }
}

sub normalize_artist_folders {
    my @newdirs = ();
TDIRLOOP: foreach my $dir ( @_ ) {
        my $ldir = $File::Find::dir.'/'.$dir;
        my @parts = split /\//, $ldir;
        next if $dir =~ /^\./;
        shift @parts;

        if ( @parts == 6 ) { ##artist folder
            #takes artist folders named "Foo, The" and renames them to "The Foo"
            if ( my ($abase) = $dir =~ m/(.+),\s+The\s*$/i ) {
                print "COPY/MOVE: $ldir -> $File::Find::dir/The $abase\n";
                dircopy( $ldir, "$File::Find::dir/The $abase" ) or die $!;  ##this doesn't merge, it copies into.., creating double album names..
                dirmove( $ldir, "$DIR->{BACKUP}/$dir" ) or die $!;
                push @newdirs, "The $abase";
            }
            else { push @newdirs, $dir }
        } else { push @newdirs, $dir; }
        
    }

    return @newdirs;
}
sub find_no_album_folder {

FDIRLOOP: foreach my $dir ( @_ ) {
        my $ldir = $File::Find::dir.'/'.$dir;

        my @parts = split /\//, $ldir;

        #print @parts."\n";
        shift @parts;
        next unless $dir !~ /^\.{1,2}$/;  #manually look in every artist folder..


        if ( @parts == 6 ) { ## artist level
            opendir PDIR, $ldir;
            while ( local $_ = readdir(PDIR) ) { 
                #print "$_\n";
                if ( ! -d "$ldir/$_" && /\.mp3$/i) {
                    #print "$ldir/$_\n";

                    my $mp3 = MP3::Tag->new("$ldir/$_");
                    $mp3->get_tags();
                    my $id3 = $mp3->{ID3v2};
                    my ($album_name, $tagname, @rest) = $id3->get_frame('TALB');
                    #print "$album_name - $tagname - @rest\n";

                    if ($album_name !~ /^\s*$/) { #do it
                        ## strip illegals..
                        $album_name =~ s/([\/\\\*\|\:"\<\>\?]|\.$)/_/g;
                        if ( -d "$ldir/$album_name" ) {
                            warn "EXISTS: $ldir/$album_name ( will merge )";
                        } else {
                            make_path( "$ldir/$album_name" ) or die "cannot make album folder $album_name";
                        }    
                        ##either dead, or the path we want exists
                        print "MOVE: $ldir/$_ -> $ldir/$album_name/$_\n";
                        move( "$ldir/$_", "$ldir/$album_name/$_" ) or die "cannot move $ldir/$_";

                        ## we moved audio, now look for an image tag and move that...
                        foreach my $ext ( qw/jpg png/ ) {
                            if ( -e "$ldir/$album_name.$ext" ) {
                                my $fname = 'folder';
                                while ( -e "$ldir/$album_name/$fname.$ext" ) {
                                    warn "$fname.$ext exists, incrementing";
                                    my ($cnt) = $fname =~ /\-(\d+)$/;
                                    if ( ! $cnt ) {
                                        $fname = 'folder-1';
                                    } else {
                                        $cnt++;
                                        $fname =~ s/\-(\d+)$/-$cnt/;
                                    }
                                } 
                                
                                print "MOVE (album art): $ldir/$album_name.$ext\n";
                                move( "$ldir/$album_name.$ext", "$ldir/$album_name/$fname.$ext") or die "cannot move $ldir/$album_name.$ext";
                            }
                        }
                    } else {
                        die "$ldir\$_ has no album tag";
                    }
                    #next FDIRLOOP ;
                } elsif ( ! -d "$ldir/$_" &&  /\.(png|jpg)/i && ! /(folder|cover)\..{3}$/ ) { ## orphaned image
                     print "OTHER: (loose image) $ldir/$_\n";
                } elsif ( ! -d "$ldir/$_" && ! /^\./ ) {
                     #cruft?
                     print "OTHER: $ldir/$_\n";
                }

            }

            closedir PDIR;
        } elsif ( @parts == 7 || @parts == 8 ) { ## album..
            opendir PDIR, $ldir;
            my @poss = ();
            while ( local $_ = readdir(PDIR) ) { 
                next if ! /\.(jpg|png|bmp)$/i;
                push @poss, $_;
            }
            closedir PDIR;
            if ( @poss ) {
                if ( @poss == 1 ) { #simple
                    my ($base,$ext) = $poss[0] =~ m/(.+)\.(.{3})$/;
                    next if $poss[0] eq 'folder.'.$ext; ## already matched how we want..
                    print "MOVE (single art) $ldir/$poss[0] -> $ldir/folder.$ext\n";
                    move( "$ldir/$poss[0]", "$ldir/folder.$ext" ) or die "$ldir/$poss[0] $!";
                } else {
                    my @front_large = grep { /(front|large|cover|folder|outside|Album\s*Art)/i } @poss;

                    my $img = Image::Magick->new;
                    my $topa = 0;
                    my @topis = ();
                    foreach my $p ( @poss ) {
                        $img->Read( "$ldir/$p" );
                        my ($w,$h) = $img->Get('width','height');
                        my $a = $w*$h;
                        if ( $a > 0 && $a >= $topa - 50 ) {
                            push( @topis, $p); 
                            $topa = $a ;
                        }
                            
                    }



                    if ( @front_large || @topis ) {
                        my %fl = map { $_ => 1 } @front_large;
                        #my $tp = map { $_ => 1 } @topis;

                        my @best = grep { $fl{$_} } @topis;
                        if ( @best ) {
                            if ( @best == 1 ) { #easy
                                _move($ldir,$best[0],'best art');
                            }  elsif ( ! grep { /^(folder|cover)\.(png|jpg)$/i } @best ) { ## i guess  we can do cleanup at some point.., but as long as we have a folder.jpg that is in the "best" group..
                                if ( ( grep { /AlbumArt/ } @best ) == @best ) {
                                    _move($ldir,$best[0],'best art');
                                } else {
                                    print "BEST: $ldir\n\t".join( "\n\t", @best )."\n";
                                }

                            }
                        } else {
                            if ( @front_large ) {
                                print "FRONT-LARGE: $ldir\n\t".join( "\n\t", @front_large )."\n";
                            } 
                            if ( @topis ) {
                                my $album_name = $parts[6];
                                my @albumn = grep { /$album_name/ } @topis;
                                if ( @albumn ) {
                                    _move($ldir,$albumn[0],'matched album name');   
                                } else {
                                    print "TOP IMAGES: $ldir\n\t".join( "\n\t", @topis )."\n";
                                }
                            }
                        }
                    } else {
                        print "$ldir\n\t".join( "\n\t", @poss )."\n";
                    }
                }
            }
        }
    }

    @_;
}
sub preproc {

PDIRLOOP: foreach my $dir ( @_ ) {
        my $ldir = $File::Find::dir.'/'.$dir;
        print "$ldir\n";
        opendir PDIR, $ldir;

        my @pictures = ();
        while ( local $_ = readdir(PDIR) ) {
            if ( /^folder.(jpg|png)$/i ) {
                print "*** has $_, skip dir\n";
                next PDIRLOOP;
            } elsif ( /^cover\.(jpg|png)$/i ) {
                print "*** rename cover to folder, skip dir\n";
                next PDIRLOOP;
            }

            next unless ! /^\./ && /\.(jpg|png)$/;
                 
            print "\t\t$_\n";
        }

        closedir PDIR;
    }
}
=head
#my $x = tie my %ARTISTS, 'DB_File', '/tmp/artist.db', O_RDWR|O_CREAT, 0666, $DB_HASH 
#        or die "Cannot open file: $!\n";




#find({  
        #wanted => \&caching_finder, 
        #wanted => \&convert_folder_art,
        #preprocess => \&find_no_album_folder,
        #preprocess => \&normalize_artist_folders,
        #postprocess => \&normalize_album_folders,
 #       }, $DIR->{SEARCH} );
find({
        wanted => \&null_wanted, 
        }, $DIR->{SEARCH} );


$z->sync;




$x->sync;

sub root {
    local $_ = lc(shift);
    s/\&/and/g;
    s/:-\(\)\[\]'//g;
    s/\s+/ /g;
    s/(^the\s+|,\s*the\s*$)//i;
    #s/[^\x00-\x7f]//g;

    return $_;
}

my %MAC = ();  ## will hold a stripped lc parent and then all matches..
my @simmacs = ();
foreach my $artist ( sort keys %ARTISTS ) {
    my $artroot = root($artist);   
    ## we can reduce this variety down to the same stripped basic.. 
    if ( defined $MAC{$artroot} ) {
        push @simmacs, $artroot;
    } 
    $MAC{$artroot}->{ARTISTS}->{$artist} = 1; 

    $MAC{$artroot}->{SIM_ALBUMS} = [] unless defined $MAC{$artroot}->{SIM_ALBUMS}; 
  
    my $albums = thaw ( $ARTISTS{$artist} ); 
    
    foreach my $album ( sort keys %{ $albums } ) { 
        my $albroot = root($album);
        if ( defined $MAC{$artroot}->{ALBUMS}->{$albroot} ) {
            push @{ $MAC{$artroot}->{SIM_ALBUMS} }, $albroot;
        }
        $MAC{$artroot}->{ALBUMS}->{$albroot}->{$album} = 1; 
    }
}

foreach my $artroot ( @simmacs ) {
    my @options = keys %{ $MAC{$artroot}->{ARTISTS} };
    print "ARTIST: $artroot -> \n\t".join("\n\t", @options )."\n";

    my @simalbs = @{ $MAC{$artroot}->{SIM_ALBUMS} };

    foreach my $albroot ( @simalbs ) {
        my @options = keys %{ $MAC{$artroot}->{ALBUMS}->{$albroot} };
        print "\tALBUM: $albroot -> \n\t\t".join("\n\t\t", @options )."\n";
    }

}



sub caching_finder {
    #! /^\./ && ! /(folder|cover)\..{3}$/ && /\.(png|jpg)/ 
    next unless /\.mp3$/i;
    my @parts = split /\//, $File::Find::name;
    shift @parts;

    #next if $parts[5] =~ /^[\.\!0-9A-K]/i;

    my $mp3 = MP3::Tag->new("$File::Find::name");
    $mp3->get_tags();
    my $id3 = $mp3->{ID3v2};
    my ($artist_name, $album_name);


    eval{ ($artist_name, $album_name) = ( $id3->artist, $id3->album ); };
    print "BAD TAG: $File::Find::name\n" && next if $@;


    my $sub;
    

    eval{ $sub = exists $ARTISTS{$artist_name} ? thaw( $ARTISTS{$artist_name} ) : {}; };
    if ( $@ ) {
        $sub = {};
        print "FAILED TO DECODE: $File::Find::name ( $@ )\n";
        ## may be funny artist name..
        $artist_name =~ s/[^\x00-\x7f]/_/g;
    }

    next if exists $sub->{$album_name};

    print "$artist_name: $album_name\n";


    $sub->{$album_name} = 1;

    $ARTISTS{$artist_name} = freeze($sub);

    $FILE_COUNT++;
    
    $x->sync if ! ( $FILE_COUNT % 100 );

}
