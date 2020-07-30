#!/usr/bin/perl
use strict;
use warnings;
use Time::Piece;
use Data::Dumper;
use DBI;

# Script to parse the Traffic data provided for WCX
# Dave O'Brien 20170922
#

my $verbose =1;
my $scats_refs = "ReferenceFile_SCATS__170810_1711.csv";
my @traffic_files = qw(2016_Q3.csv 2016_Q4.csv 2017_Q1.csv 2017_Q2.csv);
my $db = DBI->connect("dbi:SQLite:dbname=scats_counts.sqlite","","") or die $DBI::errstr;
my $do_scats = 1;
# Data structures
my %roadnumbers;
my %road_ids;
my %roadnames;
my %dates;
my %directions;
my %times;
my %linknumbers;
my %traffic_asset_keys;
my @dates;

my $firstrun = 0;
if ($firstrun) {
    my $result = drop_all_tables($db, '') or die "can't drop tables!";
    $result = make_db($db);
    dbdo($db, "BEGIN");
    my $lines = parse_file($scats_refs, "scats_asset", "scats_assets");
    print "$lines Assets\n";
    dbdo($db, "COMMIT");
    foreach my $traffic_file (@traffic_files) {
        dbdo($db, "BEGIN");
        print "$traffic_file\n" if $verbose;
        my $lines = parse_file($traffic_file, "scats_count", "scats_counts");
            print "$lines Traffic Links\n";
        dbdo($db, "COMMIT");
        # Need to add indices or this takes a very long time...
        my $result = dbdo($db, "Create Index scats_counts_idx on [scats_counts](date, time, siteID, detectorNo);", $verbose);
        $result = dbdo($db, "Create Index scats_counts_time_idx on [scats_counts](time);", $verbose);
        $result = dbdo($db, "Create Index scats_counts_SiteID_idx on [scats_counts](siteID);", $verbose);
        $result = dbdo($db, "Create Index scats_counts_detectNo_idx on [scats_counts](detectorNo);", $verbose);
        }
} else {
# read in the saved data structures from the database?
    #@road_ids = array_from_query($db, "Select distinct road_number from [traffic_counts];", $verbose);
    #my @dates = array_from_query($db, "Select distinct date from [traffic_counts];", $verbose);
    #@link_ids = array_from_query($db, "Select distinct link_number from [traffic_counts];", $verbose);
}

if ($do_scats) {
    my @counters = array_from_query($db, "select distinct TCS from [scats_assets] order by TCS;", $verbose);
    my $result = dbdo ($db, "drop table if exists [site_summary]", $verbose);
    $result = dbdo($db, "create table [site_summary] (SiteID Integer, detectorNo Integer, daily REAL, AM REAL, IP REAL, PM REAL, EV REAL)", $verbose);
    foreach my $counter (@counters){
        my @detectors = array_from_query($db, "select distinct detectorNo from [scats_assets] where TCS = $counter order by detectorNo;", $verbose);
        if ( $counter >= 769 ) {
            foreach my $detector (@detectors) {
                my $output_table = "$counter\_$detector";
                my ($UUID, $TCS, $detectorNo, $lat, $long, $street, $lane, $dir, $edgeID, $ufi) = row_from_query($db, "select * from [scats_assets] where TCS=$counter and detectorNo = $detector", $verbose);
                print "\tSCATS: $UUID, $TCS, $detectorNo, $lat, $long, $street, $lane, $dir, $edgeID, $ufi\n" if $verbose;
                my $tablename = "scats_counts";
                my $specify_link = "SiteID = $counter and detectorNo = $detector";
                make_table_for_link ($db, $tablename, $specify_link, $output_table);
                my @row = row_from_query($db, "Select sum(daily)/count(daily), sum(AM)/count(AM), sum(IP)/count(IP), sum(PM)/count(PM), sum(EV)/count(EV) from  [$output_table] where weekday in (2, 3, 4, 5, 6) and daily>0;", $verbose);
                #my @values = qw(0 0 0 0 0);
                foreach my $idx (0..4) {
                    #if (!exists($row[$idx])) {
                        $row[$idx] += 0;
                    #}
                }
                my $result = dbdo($db, "insert into [site_summary] (SiteID, detectorNo, daily, AM, IP, PM, EV) Values ($counter, $detector, $row[0], $row[1], $row[2], $row[3], $row[4])", $verbose);
            }
        }
    }

}

$db->disconnect();

## subroutines

sub make_table_for_link {
    my $db = shift;
    my $tablename = shift;
    my $specify_link = shift;
    my $output_table = shift;
    my @dates = array_from_query($db, "Select distinct date from [$tablename] where ($specify_link);", $verbose);
    my ($am, $ip, $pm, $ev) = (0,0,0,0);
    #my $verbose = 0;
    my $command = "select date, weekday from [$tablename] where ($specify_link) group by date;";
    my $weekdayhash = hash_from_query($db, $command, $verbose);
    my %weekday = %$weekdayhash;
    my $dy_hours = "";
    my $am_hours = " and time in (" . times_from_range(7..8) .")";
    my $ip_hours = " and time in (" . times_from_range(9..14) .")";
    my $pm_hours = " and time in (" . times_from_range(15..17) .")";
    my $ev_hours = " and time in (" . times_from_range(0..6) . ", " . times_from_range(18..23) .")";
    print "$am_hours\n$ip_hours\n$pm_hours\n$ev_hours\n";
    my @times = ($dy_hours, $am_hours, $ip_hours, $pm_hours, $ev_hours);
    my @volhashes;
    foreach my $time_choice (@times) {
        $command = "    select date, sum(volume) from [$tablename] where ($specify_link $time_choice) group by date;";
        push @volhashes, hash_from_query($db, $command, $verbose);
    }
    my %dy_vol = %{$volhashes[0]};
    my %am_vol = %{$volhashes[1]};
    my %ip_vol = %{$volhashes[2]};
    my %pm_vol = %{$volhashes[3]};
    my %ev_vol = %{$volhashes[4]};
    dbdo($db, "DROP table if exists [$output_table]; ", $verbose);
    dbdo($db, "create table [$output_table] (date TEXT, weekday INTEGER, daily REAL, am REAL, ip REAL, pm REAL, ev REAL);", $verbose);
    dbdo ($db, "BEGIN", $verbose);
    $verbose = 0;
    foreach my $date (sort keys % dy_vol) { #sort {$dy_vol{$a}<=>$dy_vol{$b}} %dy_vol) {
        if (exists($dy_vol{$date})) {
            my @vols = ($dy_vol{$date}, $am_vol{$date}, $ip_vol{$date}, $pm_vol{$date}, $ev_vol{$date});
            my $vols;
            #print join( ",", @vols) ."\n" if $verbose;
            foreach my $vol (@vols) {
                if (defined($vol))   {
                    $vols .= ", $vol";
                } else {
                    $vols .= ", 0";
                }
            }
            #print "$vols\n" if $verbose;
            my $command = "insert into [$output_table] (date, weekday, daily, am, ip, pm, ev)Values (\"$date\", $weekday{$date} $vols);";
            print "$command\n" if $verbose;
            dbdo($db, $command, $verbose);
        }
    }
    $verbose = 1;
    dbdo ($db, "commit", $verbose);

}
sub parse_file {
    # parse a file generally
    my $file = shift;
    my $type = shift;
    my $tablename = shift;
    my @assets;
    open (my $infh, "<", $file);
    my $headers = <$infh>;
    chomp $headers;
    my @headers = split "[~,]", $headers;
    print "File: $file: \nType: $type\nKeys: " . join(", ", @headers) ."\n\n"  if $verbose;
    my $UUID = 0;
    while (my $line = <$infh>) {
        chomp $line;
        if ( $line =~ /\"/) {
            # remove the commas between quotes, and the quotes
            my @parts  = split '"', $line;
            if ($#parts !=2) {print "CHECK: $line\n";}
            $parts[1] =~ s/,/;/g;
            $line = join ('', @parts);
        }
        if ($line =~ /,$/) {
            $line =~ s/,$/,0/;
        }
        if ($type eq "scats_asset") {$line = sprintf("%d", $UUID) . ',' . $line;}
        my @line = split "[~,]", $line;
        my $asset;
        if ($type eq "traffic_asset"){ $asset = traffic_asset->new($db, $tablename, @line);}
        if ($type eq "mway_asset")   { $asset = mway_asset->new($db, $tablename, @line);}
        if ($type eq "scats_asset")   { $asset = scats_asset->new($db, $tablename, @line);}
        if ($type eq "traffic_count"){ $asset = traffic_count->new($db, $tablename, @line);}
        if ($type eq "mway_count")   { $asset = mway_count->new($db, $tablename, @line);}
        if ($type eq "scats_count")   { $asset = scats_count->new($db, $tablename, @line);}
        #push @assets, $asset;
        #print "$type:$tablename:$#line:    @line \n" if $verbose;
        #print Dumper($asset) if $verbose;
        #$roadnames{$asset->{road_name}}++ if defined($asset->{road_name});
        #$roadnumbers{$asset->{road_number}}++ if defined($asset->{road_number});
        #$linknumbers{$asset->{link_number}}++ if defined($asset->{link_number});
        #$directions{$asset->{link_number}}++ if defined($asset->{link_number});
        #$traffic_asset_keys{$asset->{traffic_asset_key}}++ if defined($asset->{traffic_asset_key});
        #$dates{$asset->{date}}++ if defined($asset->{date});
        #$times{$asset->{time}}++ if defined($asset->{time});
        #if (defined($asset->{road_number}) && defined($asset->{link_number})){
        #    my $road_id = $asset-> {road_number}."~".$asset->{link_number};
        #    $road_ids{$road_id}++;
        #}
        $UUID++;
    }
    close $infh;
    #print Dumper ($assets[0]) if $verbose;
    return $UUID;
}
##
sub printall {
    # print all of the key/value combinations from a hash to the file
    # and save them to the database
    my $outfile = shift; # Take the first item passed in the implicit array
    my $hashref = shift;
    my %hash = %$hashref;
    my $verbose = 0;
    my ($tablename, $null) = split ("[.]", $outfile);
    print "Outfile: $outfile, $tablename\n" if $verbose;
    my $result = dbdo($db, "CREATE TABLE if NOT EXISTS [$tablename] ($tablename TEXT PRIMARY KEY, Count INTEGER);", $verbose);
    dbdo($db, "BEGIN", $verbose);
    open (my $outfh, ">", $outfile);
    foreach my $key (keys %hash) {
        if (exists($hash{$key})) {
            print $outfh "$key: $hash{$key}\n";
            dbdo($db, "INSERT into [$tablename] ($tablename, count) Values (\"$key\", $hash{$key});", $verbose);
        }
    }
    close $outfh;
    dbdo($db, "COMMIT", $verbose);
}
sub times_from_range {
    my $result;
    foreach my $time (@_) {
        $result .= "'" . sprintf ("%02d", $time) . ":00:00', ";
    }
    $result =~ s/, $//;
    return $result;
}
# Database stuff
sub make_db {
    #Make the Database Structure
    print "making the database: $db\n" if $verbose;

        #"traffic_assets"=>"traffic_asset_key INTEGER PRIMARY KEY, wgs84_lat REAL, wgs84_lon REAL, road_name TEXT, rta_region TEXT, road_number TEXT, link_number TEXT, carriageway TEXT",
        #"rms_assets"=>"traffic_asset_key INTEGER PRIMARY KEY, wgs84_lat REAL, wgs84_lon REAL, road_name TEXT, rta_region TEXT, road_number TEXT, link_number TEXT, carriageway TEXT",
        #"mway_assets"=>"counter_id TEXT PRIMARY KEY, road_number INTEGER, link_number INTEGER, carriageway TEXT, direction TEXT, additional_info TEXT, lat REAL, lon REAL",
        #"traffic_counts"=>"date TEXT, time TEXT, weekday INTEGER,traffic_asset_key TEXT, direction INTEGER, volume REAL, confidence REAL, road_number Text, link_number Text, carriageway TEXT",
        #"mway_counts"=>"date TEXT, time TEXT, weekday INTEGER, road_number TEXT, link_number TEXT, carriageway TEXT, counter_id TEXT, volume REAL, confidence REAL",
    my %tables = (
        "scats_assets"=>"UUID INTEGER PRIMARY KEY, TCS INTEGER, detectorNo INTEGER, lat REAL, long, REAL,
        street TEXT, lane INTEGER, dir TEXT, edgeID INTEGER, ufi INTEGER",
        "scats_counts"=>"Date TEXT, time TEXT, weekday INTEGER, SiteID INTEGER, detectorNo INTEGER, volume REAL, confidence REAL");
    foreach my $tablename (%tables) {
        if (exists $tables{$tablename} ) {
            my $command = "Create Table if not exists [$tablename] ($tables{$tablename})";
            my $result = dbdo($db, $command, $verbose);
        }
    }
    #build_tables_from_files($db);
}
sub drop_all_tables {
    # get a list of table names from $db and drop them all
    my $db = shift;
    my $prefix = shift;
    my @tables;
    my $query = querydb($db, "select name from sqlite_master where type='table' and name like '$prefix%' order by name", 1);
    # we need to extract the list of tables first - sqlite doesn't like
    # multiple queries at the same time.
    while (my @row = $query->fetchrow_array) {
        push @tables, $row[0];
    }
    dbdo ($db, "BEGIN", 1);
    foreach my $table (@tables) {
        dbdo ($db, "DROP TABLE if Exists [$table]", 1);
    }
    dbdo ($db, "COMMIT", 1);
    return 1;
}
sub dbdo {
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    if (length($command) > 1000000) {
        die "$command too long!";
    }
    print "\t$db: ".length($command)." $command\n" if $verbose;
    my $result = $db->do($command) or die $db->errstr . "\nwith: $command\n";
    return $result;
}
sub querydb {
    # prepare and execute a query
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    print "\tQUERYDB: $command\n" if $verbose;
    my $query = $db->prepare($command) or die $db->errstr;
    $query->execute or die $query->errstr;
    return $query;
}
sub array_from_query {
    # return an array from a query which results in one item per line
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    my @results;
    my $query = querydb($db, $command, $verbose);
    while (my @row = $query->fetchrow_array) {
        push @results, $row[0];
    }
    return (@results);
}
sub hash_from_query {
    # return an array from a query which results in two items per line
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    my %results;
    my $query = querydb($db, $command, $verbose);
    while (my @row = $query->fetchrow_array) {
        $results{$row[0]} = $row[1];
    }
    #print Dumper(%results);
    return (\%results);
}
sub row_from_query {
    # return a single row response from a query (actully, the first row)
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    my $query = querydb($db, $command, $verbose);
    my @results = $query->fetchrow_array;
    return (@results);
}

## Objects
package traffic_asset;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my @data = @_;
    my $self = {
        traffic_asset_key => shift,
        wgs84_lat_current => shift,
        wgs84_lon_current => shift,
        road_name => shift,
        rta_region => shift,
        road_number => shift,
        link_number => shift,
        carriageway => shift
    };
    bless $self, $class;
    if (@data == 8 and $db != 0) {
        $data[5] = sprintf("%07d", $data[5]);
        $data[6] = sprintf("%04d", $data[6]);
        my $result = main::dbdo($db, "Insert into [$tablename] (traffic_asset_key, wgs84_lat, wgs84_lon, road_name, rta_region, road_number, link_number, carriageway) Values ($data[0], $data[1], $data[2], \"$data[3]\", \"$data[4]\", \"$data[5]\", \"$data[6]\", \"$data[7]\")", 0);
    }
    return $self;
}
package mway_asset;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my @data = @_;
    my $self = {
        counter_id => shift,
        road_number => shift,
        link_number => shift,
        carriageway => shift,
        direction => shift,
        additional_info => shift,
        lat => shift,
        lon => shift
    };
    bless $self, $class;
    if (@data == 8 ) {
        $data[1] = sprintf("%07d", $data[1]);
        $data[2] = sprintf("%04d", $data[2]);
        my $result = main::dbdo($db, "Insert into [$tablename] (counter_id, road_number, link_number, carriageway, direction, additional_info, lat, lon) Values (\"$data[0]\", \"$data[1]\", \"$data[2]\", \"$data[3]\", \"$data[4]\", \"$data[5]\", $data[6], $data[7])", 0);
    }
    return $self;
}
package scats_asset;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my @data = @_;
    my $self = {
        UUID => shift,
        TCS => shift,
        detectorNo => shift,
        lat => shift,
        long => shift,
        street => shift,
        lane => shift,
        dir => shift,
        edgeID => shift,
        ufi => shift
    };
    my $verbose = 0;
    bless $self, $class;
    if (@data == 10 ) {
        #$data[1] = sprintf("%07d", $data[1]);
        #$data[2] = sprintf("%04d", $data[2]);
        $data[9] += 0;
        my $result = main::dbdo($db, "Insert into [$tablename] (UUID, TCS, detectorNo, lat, long, street, lane, dir, edgeID, ufi) Values ($data[0], $data[1], $data[2], $data[3], $data[4], \"$data[5]\", $data[6], \"$data[7]\", $data[8], $data[9])", $verbose);
    }
    return $self;
}
package traffic_count;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my $datetime = shift;
    my ($date, $time) = split " ", $datetime;
    my $surveytime = Time::Piece->strptime("$date $time", "%Y-%m-%d %H:%M:%S");
    my $weekday = $surveytime->wday; # Sunday = 1, Sat = 7
    my @data = ($date, $time, @_);
    my $self = {
        date => $date,
        time => $time,
        weekday => $weekday,
        traffic_asset_key => shift,
        direction => shift,
        volume => shift,
        confidence => shift,
        road_number => shift,
        link_number => shift,
        carriageway => shift
    };
    bless $self, $class;
    while (@data < 9) { push @data, 0;}
    if (@data ==  9 ) {
        my $result = main::dbdo($db, "Insert into [$tablename] (date, time, weekday, traffic_asset_key, direction, volume, confidence, road_number, link_number, carriageway) Values (\"$data[0]\", \"$data[1]\", $weekday, $data[2], $data[3], $data[4], $data[5], \"$data[6]\", \"$data[7]\", \"$data[8]\")", 0);
    }
    return $self;
}
package mway_count;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my $datetime = shift;
    my ($date, $time) = split " ", $datetime;
    my @data = ($date, $time, @_);
    my $surveytime = Time::Piece->strptime("$date $time", "%Y-%m-%d %H:%M:%S");
    my $weekday = $surveytime->wday; # Sunday = 1, Sat = 7
    my $self = {
        date => $date,
        time => $time,
        weekday => $weekday,
        road_number => shift,
        link_number => shift,
        carriageway => shift,
        counter_id => shift,
        volume => shift,
        confidence => shift
    };
    bless $self, $class;
    if (@data == 8 ) {
        my $result = main::dbdo($db, "Insert into [$tablename] (date, time, weekday, road_number, link_number, carriageway, counter_id, volume, confidence) Values (\"$data[0]\", \"$data[1]\", $weekday, \"$data[2]\", \"$data[3]\", \"$data[4]\", \"$data[5]\", $data[6], $data[7])", $verbose);
    }
    return $self;
}
package scats_count;
sub new {
    my $class = shift;
    my $db = shift;
    my $tablename = shift;
    my $datetime = shift;
    my ($date, $time) = split " ", $datetime;
    my @data = ($date, $time, @_);
    my $surveytime = Time::Piece->strptime("$date $time", "%Y-%m-%d %H:%M:%S");
    my $weekday = $surveytime->wday; # Sunday = 1, Sat = 7
    my $self = {
        date => $date,
        time => $time,
        weekday => $weekday,
        siteID => shift,
        detectorNo => shift,
        volume => shift,
        confidence => shift
    };
    bless $self, $class;
    #print "#". scalar(@data).":$#data:@data\n " if $verbose;
        my $verbose = 0;
    if (scalar(@data) == 6 ) {
        my $command = "Insert into [$tablename] (date, time, weekday, siteID, detectorNo, volume, confidence) Values (\"$data[0]\", \"$data[1]\", $weekday, $data[2], $data[3], $data[4], $data[5])";
        #print "$command\n";
        my $result = main::dbdo($db, "Insert into [$tablename] (date, time, weekday, siteID, detectorNo, volume, confidence) Values (\"$data[0]\", \"$data[1]\", $weekday, $data[2], $data[3], $data[4], $data[5])", $verbose);
    }
    return $self;
}
