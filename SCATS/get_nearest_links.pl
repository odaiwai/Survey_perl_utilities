#!/usr/bin/perl
use strict;
use warnings;
use XBase;
use DBI;

# Script to read in the databases of nodes, links and counts
# and match up the counts to the links
#
# Dave O'Brien 20170929

my $verbose = 1;
my $firstrun = 0;
my ($do_mway, $do_rms) = (0,1);


my $db = DBI->connect("dbi:SQLite:dbname=traffic_counts.sqlite","","") or die $DBI::errstr;

# Read in the dbf files
print "XBase Version: $XBase::VERSION\n" if $verbose;
my %addopts = ();
$addopts{'ignorememo'} = 1;
$addopts{'ignorebadheader'} = 1;
my $netfile = "LINKS.DBF";
my $nodefile = "NODES.DBF";
my $rms_file = "RMS_reference.dbf";
my $mwayfile = "Motorway_reference.dbf";
# Read in the nodes and links and make a sqlite tables from them
my $link_db = new XBase 'name' => $netfile, %addopts;
my $node_db = new XBase 'name' => $nodefile, %addopts;
my $rms_db = new XBase 'name' => $rms_file, %addopts;
my $mway_db = new XBase 'name' => $mwayfile, %addopts;
my $result = make_db($db, $verbose) if ($firstrun);
$link_db->close;
$node_db->close;
$rms_db->close;
$mway_db->close;


# Algorithm:
# read in the traffic assets info for the model X,Y
# for each Traffic Asset:
#   get the coords in WGS84 56S
#   find the closest link mid point
#   Assign that

$result = make_tables();
$result = read_in_known_links($db, "known_mway_links.csv", "known_mway_links");
$result = read_in_known_links($db, "known_rms_links.csv", "known_rms_links");

if ($do_mway) {
    my @counters = array_from_query($db, "select distinct counter_id from net_mway_sites order by counter_id;", $verbose);
    dbdo($db, "BEGIN", $verbose);
    foreach my $counter (@counters) {
        my ($direction, $additional) = row_from_query($db, "select direction, additional_info from [mway_assets] where counter_id = '$counter';", $verbose);
        my ($roadnum, $linknum, $carriageway, $x, $y) = row_from_query($db, "Select road_numbe, link_numbe, carriagewa, x, y from [net_mway_sites] where counter_id = '$counter';", $verbose);
        $roadnum = sprintf("%07d", $roadnum);
        $roadnum = sprintf("%04d", $linknum);
        if (!(defined($x)) and !(defined($y))) { exit;}
        print "$counter, $roadnum, $linknum, $carriageway, $x, $y\n"if $verbose;
        my ($possible_a, $possible_b, $shortest_distance) = closest_link_to_node($db, $counter, $x, $y, "known_mway_links");
        dbdo($db, "Insert into [distances] (UUID, direction, additional, road_number, link_number, carriageway, anode, bnode, distance) Values (\"$counter\", \"$direction\", \"$additional\", \"$roadnum\", \"$linknum\", \"$carriageway\", $possible_a, $possible_b, $shortest_distance);", $verbose);
        my (@counts) = row_from_query($db, "select sum(daily)/count(daily), sum(am)/count(am), sum(ip)/count(ip), sum(pm)/count(pm), sum(ev)/count(ev) from [$counter\_$direction] where (weekday in (2, 3, 4, 5, 6) and date like '2014%');", $verbose);
        my $counts = sanitise_values (@counts);
        dbdo ($db, "insert or replace into [mway_surveys] (anode, bnode, UUID, direction, additional, distance_from_site, daily, am, ip, pm, ev) Values ($possible_a, $possible_b, \"$counter\", \"$direction\", \"$additional\", $shortest_distance $counts)", $verbose);
        exit;
    }
    dbdo($db, "COMMIT", $verbose);
    $result = `sqlite3 traffic_counts.sqlite -header -csv "select * from mway_surveys order by UUID;" > mway_surveys.csv`;
}
if ($do_rms) {
    my @TAKs = array_from_query($db, "select distinct traffic_asset_key from [traffic_counts] where volume>0;", $verbose);
    dbdo($db, "BEGIN", $verbose);
    foreach my $TAK (@TAKs) {
        my ($roadnum, $linknum, $carriageway, $x, $y) = row_from_query($db, "Select road_numbe, link_numbe, carriagewa, x, y from [net_rms_sites] where TRAFFIC_AS = '$TAK';", $verbose);
        $roadnum = sprintf("%07d", $roadnum);
        $linknum = sprintf("%04d", $linknum);
        if (!(defined($x)) and !(defined($y))) { exit;}
        print "\t$TAK, $roadnum, $linknum, $carriageway, $x, $y\n"if $verbose;
        my ($possible_a, $possible_b, $shortest_distance) = closest_link_to_node($db, $TAK, $x, $y, "known_rms_links");
        print "\t$TAK: $possible_a, $possible_b, $shortest_distance\n" if $verbose;
        my @directions = array_from_query($db, "Select distinct direction from [traffic_counts] where (road_number = '$roadnum' and link_number = '$linknum' and carriageway = '$carriageway')", $verbose);
        foreach my $direction (@directions) {
            dbdo($db, "Insert into [distances] (UUID, direction, road_number, link_number, carriageway, anode, bnode, distance) Values (\"$TAK\_$direction\", \"$direction\", \"$roadnum\", \"$linknum\", \"$carriageway\", $possible_a, $possible_b, $shortest_distance);", $verbose);
            foreach my $year (2015..2017) {
                print "\t$year: $direction\n" if $verbose;
                my (@counts) = row_from_query($db, "select sum(daily)/count(daily), sum(am)/count(am), sum(ip)/count(ip), sum(pm)/count(pm), sum(ev)/count(ev) from [$TAK\_$direction] where (weekday in (2, 3, 4, 5, 6) and date like '$year%');", $verbose);
                my $counts = sanitise_values (@counts);
                dbdo ($db, "insert or replace into [Y$year\_surveys] (anode, bnode, UUID, direction, distance_from_site, daily, am, ip, pm, ev) Values ($possible_a, $possible_b, \"$TAK\", \"$direction\", $shortest_distance $counts)", $verbose);
            }
        }
    }
        dbdo($db, "COMMIT", $verbose);
    foreach my $year (2015..2017) {
        $result = `sqlite3 traffic_counts.sqlite -header -csv "select * from Y$year\_surveys order by UUID;" > y$year\_surveys.csv`;
    }
}

$db->disconnect;

# Subroutines
sub make_db {
    my $db = shift;
    my $verbose = shift;
    #my $result = make_tables($db, "net_", $verbose);
    convert_dbf_to_table($db, $link_db, "net_links", $verbose);
    convert_dbf_to_table($db, $node_db, "net_nodes", $verbose);
    convert_dbf_to_table($db, $rms_db, "net_rms_sites", $verbose);
    convert_dbf_to_table($db, $mway_db, "net_mway_sites", $verbose);
    dbdo($db, "begin", $verbose);
    dbdo($db, "drop table if exists [midpoints]", $verbose);
    dbdo($db, "create table midpoints AS select a, b, (select x-3 from [net_nodes] where n = a) as ax,  (select y-3 from [net_nodes] where n = a) as ay, (select x-3 from [net_nodes] where n = b) as bx, (select y-3 from [net_nodes] where n = b) as by from [net_links];", $verbose);
    dbdo($db, "COMMIT", $verbose);
    return 1;
}
sub convert_dbf_to_table {
    my $db = shift;
    my $dbf = shift;
    my $tablename = shift;
    my $verbose = shift;
    print "converting $dbf to $db.$tablename\n" if $verbose;
    # Drop the table and recreate it from the dbf
    dbdo ($db, "DROP TABLE if Exists [$tablename]", $verbose);
    my @field_names = $dbf->field_names();
    my @field_types = $dbf->field_types();
    #print join(",", @field_names) . "\n";
    #print join(",", @field_types) . "\n";
    my @field_decimals = $dbf->field_decimals();
    my $command = "CREATE TABLE IF NOT EXISTS [$tablename] (";
    my $numfields = scalar(@field_names)-1;
    foreach (0..$numfields) {
        $command .= ", " if ($_>0);
        $command .= lc($field_names[$_]);
        if ($field_types[$_] eq "C") {
            $command .= " Text";
        }
        if ($field_types[$_] eq "N") {
            if ($field_decimals[$_] > 0) {
                $command .=" Real";
            } else {
                $command .=" Real";
            }
        }
        #print "$_: $command\n" if $verbose;
    }
    $command .= ");";
    print "$command\n" if $verbose;
    my $result = dbdo($db, $command, $verbose);
    # Populate the database
    my $field_names = join(", ", @field_names);
    print "$numfields: $field_names\n" if $verbose;
    print "$numfields: " . join(",", @field_types) ."\n" if $verbose;
    my $cursor = $dbf->prepare_select();
    dbdo($db, "BEGIN", $verbose);
    while (my @data = $cursor->fetch) {
        my $values;
        foreach (0..$numfields) {
            $values .= ", " if ($_>0);
            if ($field_types[$_] eq "C") {
                $values .= "\"$data[$_]\"";
            } else {
                if (defined($data[$_])) {
                    $values .= "$data[$_]" ;
                } else {
                    $values .= "0" ;
                }
            }
        }
        my $command = "Insert or replace into [$tablename] ($field_names) Values ($values)";
        dbdo($db, $command, 0);
    }
    dbdo($db, "COMMIT", $verbose);
}
# database subs
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
sub row_from_query {
    # return a single row response from a query (actully, the first row)
    my $db = shift;
    my $command = shift;
    my $verbose = shift;
    my $query = querydb($db, $command, $verbose);
    my @results = $query->fetchrow_array;
    return (@results);
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
sub make_tables {
    my %tables = (
        "distances" => "UUID Text Primary Key, direction Text, additional Text, road_number TEXT, link_number TEXT, carriageway TEXT, anode Integer, bnode Integer, distance Real",
        "mway_surveys"=> "anode Integer, bnode Integer, UUID Text Primary Key, direction Text, additional Text, distance_from_site Real, daily REAL, am REAL, ip REAL, pm REAL, ev REAL",
        "Y2015_surveys"=> "anode Integer, bnode Integer, UUID Text Primary Key, direction Text, distance_from_site Real, daily REAL, am REAL, ip REAL, pm REAL, ev REAL",
        "Y2016_surveys"=> "anode Integer, bnode Integer, UUID Text Primary Key, direction Text, distance_from_site Real, daily REAL, am REAL, ip REAL, pm REAL, ev REAL",
        "Y2017_surveys"=> "anode Integer, bnode Integer, UUID Text Primary Key, direction Text, distance_from_site Real, daily REAL, am REAL, ip REAL, pm REAL, ev REAL");
    foreach my $tablename (%tables) {
        if (exists $tables{$tablename} ) {
            my $result = dbdo($db, "Drop Table if exists [$tablename];", $verbose);
            my $command = "Create Table if not exists [$tablename] ($tables{$tablename})";
            $result = dbdo($db, $command, $verbose);
        }
    }
}

sub read_in_known_links {
    my $db = shift;
    my $file = shift;
    my $tablename = shift;
    my $result = dbdo($db, "Drop Table if exists [$tablename];", $verbose);
    $result = dbdo($db, "Create Table if not exists [$tablename] (anode Integer, bnode Integer, UUID Text Unique Primary Key, direction Text, additional Text);", $verbose);
    dbdo($db, "BEGIN", $verbose);
    open (my $infh, "<", $file);
    while (my $line = <$infh>) {
        chomp $line;
        $line =~ s/ //g;
        $line =~ s/-/0/g;
        my($anode, $bnode, $UUID, $direction, $additional) = split ",", $line;
        my $result = dbdo($db, "Insert or Replace into [$tablename] (UUID, anode , bnode ) Values (\"$UUID\", $anode, $bnode);", $verbose);
    }
    dbdo($db, "COMMIT", $verbose);
    close $infh;
    $result = dbdo($db, "select count(*) from [$tablename]", $verbose);
    print "$result known links imported to [$tablename]";
    return $result;
}

sub sanitise_values {
    my $counts = "";
    foreach my $count(@_) {
        if (defined($count)) {
            $counts .= ", $count";
        } else {
            $counts .= ", 0";
        }
    }
    #print "$counts\n" if $verbose;
    return $counts;
}

sub closest_link_to_node {
    my $db = shift;
    my $counter = shift;
    my $x = shift;
    my $y = shift;
    my $known_links_table = shift;
    my ($known_a, $known_b) = row_from_query($db, "Select anode, bnode from [$known_links_table] where UUID like '$counter';", $verbose);
    my ($possible_a, $possible_b, $shortest_distance) = (0,0,100000000);
    if ( defined($known_a) and defined($known_b)) {
        $possible_a = $known_a;
        $possible_b = $known_b;
        $shortest_distance = -1;
        print "\tKnown Link: $known_a, $known_b\n" if $verbose;
    } else {
        my $query = querydb($db, "select a, b, ax, ay, bx, by from [midpoints] where (a>9999 and b>9999);", $verbose);
        while (my @row = $query->fetchrow_array) {
            my ($anode, $bnode, $ax, $ay, $bx, $by) = @row;
            # formula from https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
            # t check from https://math.stackexchange.com/questions/2248617/shortest-distance-between-a-point-and-a-line-segment
            my $length = sqrt(($bx-$ax)**2 + ($by-$ay)**2);
            my $distance = abs(($bx-$ax)*($ay-$y)-($ax-$x)*($by-$ay))/$length;
            my $t = -(($bx-$ax)*($ax-$x)+($by-$ay)*($ay-$y))/(($bx-$ax)**2+($by-$ay)**2);
            #print "$anode, $bnode, $ax, $ay, $bx, $by, $length, $distance\n" ;
            if ( ($t >= 0 and $t <= 1 ) and $distance < $shortest_distance) {
                $possible_a = $anode;
                $possible_b = $bnode;
                $shortest_distance = $distance;
                print "$possible_a, $possible_b, $distance\n" if $verbose;
            }
        }
    }
    print "\t$counter: $possible_a, $possible_b, $shortest_distance\n" if $verbose;
    return ($possible_a, $possible_b, $shortest_distance);
}