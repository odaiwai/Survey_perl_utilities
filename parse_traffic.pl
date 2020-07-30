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
my $assets_refs = "traffic_assets_info_v2.csv";
my $rms_refs = "ReferenceFile_RMSStations_070701-170630__170728_1732.csv";
my $motorway_refs ="ReferenceFile_Motorways_6004_6005_0651__170817_1623.csv";
my $traffic_file = "traffic_v2.csv";
my $motorway_file = "MOTORWAY_HOURLY_completed.csv";
my $db = DBI->connect("dbi:SQLite:dbname=traffic_counts.sqlite","","") or die $DBI::errstr;
my ($do_mway, $do_rms) = (0,1);
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

#my ($TAK, $dir1, $dir2) = ("77315", "8004", "8124");
#make_table_for_link($db, "traffic_counts", "traffic_asset_key = '$TAK' and direction = '$dir1'", "$TAK\_$dir1");
#make_table_for_link($db, "traffic_counts", "traffic_asset_key = '$TAK' and direction = '$dir2'", "$TAK\-$dir2");
#exit;

my $firstrun = 0;
if ($firstrun) {
    my $result = drop_all_tables($db, '') or die "can't drop tables!";
    $result = make_db($db);
    dbdo($db, "BEGIN");
    my @traffic_assets = parse_file($assets_refs, "traffic_asset", "traffic_assets");
    print "$#traffic_assets Assets\n";
    my @rms_refs = parse_file($rms_refs, "traffic_asset", "rms_assets");
    print "$#rms_refs RMS Refs\n";
    my @mway_refs = parse_file($motorway_refs, "mway_asset", "mway_assets");
    print "$#mway_refs MWAY Refs\n";
    dbdo($db, "COMMIT");

    dbdo($db, "BEGIN");
    my @traffic = parse_file($traffic_file, "traffic_count", "traffic_counts");
    print "$#traffic Traffic Links\n";
    dbdo($db, "COMMIT");

    dbdo($db, "BEGIN");
    my @mway_traffic = parse_file($motorway_file, "mway_count", "mway_counts");
    print "$#mway_traffic MwayTraffic Links\n";
    dbdo($db, "COMMIT");
    printall("roadnumbers.csv", \%roadnumbers);
    printall("roadnames.csv", \%roadnames);
    printall("linknumbers.csv", \%linknumbers);
    printall("traffic_asset_keys.csv", \%traffic_asset_keys);
    printall("dates.csv", \%dates);
    printall("directions.csv", \%dates);
    printall("road_ids.csv", \%road_ids);


} else {
# read in the saved data structures from the database?
    #@road_ids = array_from_query($db, "Select distinct road_number from [traffic_counts];", $verbose);
    #my @dates = array_from_query($db, "Select distinct date from [traffic_counts];", $verbose);
    #@link_ids = array_from_query($db, "Select distinct link_number from [traffic_counts];", $verbose);
}

#my @road_ids = ("0006092~0010", "0000200~0140", "0000002~0198", "0000165~0240"); # to do a few cases

# Now do it again by motorway ID
if ($do_mway) {
    my @counters = array_from_query($db, "select distinct counter_id from mway_assets;", $verbose);
    foreach my $counter (@counters){
        my @row = row_from_query($db, "select * from [mway_assets] where counter_id = '$counter';", $verbose);
        my ($counter_id,$road_number,$link_number,$carriageway,$direction,$additional_info,$lat,$lon) = @row;
        $carriageway = substr($carriageway, 0, 1);
        $road_number = sprintf("%07d", $road_number);
        $link_number = sprintf("%04d", $link_number);
        print "\tMWAY: $counter_id,$road_number,$link_number,$carriageway,$direction,$additional_info,$lat,$lon\n" if $verbose;
        my $tablename = "mway_counts";
        #my $specify_link = "road_number = '$road_number' and link_number = '$link_number'";
        #$specify_link .= "and carriageway = '$carriageway'";
        my $specify_link = "counter_id = '$counter'";
        my $output_table = "$counter_id\_$direction";
        make_table_for_link ($db, $tablename, $specify_link, $output_table);
    }
}

# better to do it by RMS Traffic Asset Key
if ($do_rms) {
    my @TAKs = array_from_query($db, "select distinct traffic_asset_key from [traffic_counts];", $verbose);
    foreach my $TAK (@TAKs) {
        my @row = row_from_query($db, "select * from [rms_assets] where traffic_asset_key = '$TAK';", $verbose);
        my ($rms_asset, $lat, $long, $roadname, $region, $roadnum, $linknum, $carriageway) = @row;
        $carriageway = substr($carriageway, 0, 1);
        print "\tTAK: $rms_asset, $lat, $long, $roadname, $region, $roadnum, $linknum, '$carriageway'\n" if $verbose;
        my $tablename = "traffic_counts";
        my @directions = array_from_query($db, "Select distinct direction from [$tablename] where (traffic_asset_key = '$TAK')", $verbose);
        print "\tDIRECTIONS: @directions\n" if $verbose;
        foreach my $direction (@directions) {
            my $specify_link = "traffic_asset_key = '$TAK' and direction = '$direction'";
            my $output_table = "$TAK\_$direction";
            make_table_for_link ($db, $tablename, $specify_link, $output_table);
        }

    }
}
# Do something with them
##
#foreach road_number~link_number:
#   foreach date:
#       calculate AM, IP, PM, EV
#       tabulate traffic_asset_key, x, y, weekday, roadnumber, linknumber,

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
        $command = "select date, sum(volume) from [$tablename] where ($specify_link $time_choice and volume>0) group by date;";
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
    while (my $line = <$infh>) {
        chomp $line;
        if ( $line =~ /\"/) {
            # remove the commas between quotes, and the quotes
            my @parts  = split '"', $line;
            if ($#parts !=2) {print "CHECK: $line\n";}
            $parts[1] =~ s/,/;/g;
            $line = join ('', @parts) if $verbose;
        }
        my @line = split "[~,]", $line;
        my $asset;
        if ($type eq "traffic_asset"){ $asset = traffic_asset->new($db, $tablename, @line);}
        if ($type eq "mway_asset")   { $asset = mway_asset->new($db, $tablename, @line);}
        if ($type eq "traffic_count"){ $asset = traffic_count->new($db, $tablename, @line);}
        if ($type eq "mway_count")   { $asset = mway_count->new($db, $tablename, @line);}
        push @assets, $asset;
        #print Dumper($asset) if $verbose;
        $roadnames{$asset->{road_name}}++ if defined($asset->{road_name});
        $roadnumbers{$asset->{road_number}}++ if defined($asset->{road_number});
        $linknumbers{$asset->{link_number}}++ if defined($asset->{link_number});
        $directions{$asset->{link_number}}++ if defined($asset->{link_number});
        $traffic_asset_keys{$asset->{traffic_asset_key}}++ if defined($asset->{traffic_asset_key});
        $dates{$asset->{date}}++ if defined($asset->{date});
        $times{$asset->{time}}++ if defined($asset->{time});
        if (defined($asset->{road_number}) && defined($asset->{link_number})){
            my $road_id = $asset-> {road_number}."~".$asset->{link_number};
            $road_ids{$road_id}++;
        }
    }
    close $infh;
    print Dumper ($assets[0]) if $verbose;
    return (@assets);
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
    my %tables = (
        "traffic_assets"=>"traffic_asset_key INTEGER PRIMARY KEY, wgs84_lat REAL, wgs84_lon REAL, road_name TEXT, rta_region TEXT, road_number TEXT, link_number TEXT, carriageway TEXT",
        "rms_assets"=>"traffic_asset_key INTEGER PRIMARY KEY, wgs84_lat REAL, wgs84_lon REAL, road_name TEXT, rta_region TEXT, road_number TEXT, link_number TEXT, carriageway TEXT",
        "mway_assets"=>"counter_id TEXT PRIMARY KEY, road_number INTEGER, link_number INTEGER, carriageway TEXT, direction TEXT, additional_info TEXT, lat REAL, lon REAL",
        "traffic_counts"=>"date TEXT, time TEXT, weekday INTEGER,traffic_asset_key TEXT, direction INTEGER, volume REAL, confidence REAL, road_number Text, link_number Text, carriageway TEXT",
        "mway_counts"=>"date TEXT, time TEXT, weekday INTEGER, road_number TEXT, link_number TEXT, carriageway TEXT, counter_id TEXT, volume REAL, confidence REAL");
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
        my $result = main::dbdo($db, "Insert into [$tablename] (date, time, weekday, road_number, link_number, carriageway, counter_id, volume, confidence) Values (\"$data[0]\", \"$data[1]\", $weekday, \"$data[2]\", \"$data[3]\", \"$data[4]\", \"$data[5]\", $data[6], $data[7])", 0);
    }
    return $self;
}
