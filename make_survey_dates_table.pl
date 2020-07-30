#!/usr/bin/perl
use strict;
use warnings;
use DBI;

# Script to make a table of survey dates by year
#
# Dave O'Brien 20171017

my $verbose = 1;
my $firstrun = 0;
my ($do_mway, $do_rms) = (0,1);

my $db = DBI->connect("dbi:SQLite:dbname=traffic_counts.sqlite","","") or die $DBI::errstr;

# Algorithm:

my $result = make_tables();

if ($do_mway) {
    my @counters = array_from_query($db, "select distinct counter_id from net_mway_sites order by counter_id;", $verbose);
    dbdo($db, "BEGIN", $verbose);
    foreach my $counter (@counters) {
        my $y2014_start = row_from_query($db, "select Date from [$counter] order by Date asc limit 1;", $verbose);
        my $y2014_end = row_from_query($db, "select Date from [$counter] order by Date asc limit 1;", $verbose);
        print "$counter, $y2014_start, $y2014_end\n" if $verbose;
        dbdo ($db, "insert or replace into [SurveyDates] (UUID, direction, Y2014_start, Y2014_end) Values (\"$y2014_start\", \"$y2014_end\")", $verbose);
    }
    dbdo($db, "COMMIT", $verbose);
}

if ($do_rms) {
    my @TAKs = array_from_query($db, "select distinct traffic_asset_key from [traffic_counts] where volume>0 order by traffic_asset_key asc;", $verbose);
    foreach my $TAK (@TAKs) {
        dbdo($db, "BEGIN", $verbose);
        my @directions = array_from_query($db, "Select distinct direction from [traffic_counts] where (traffic_asset_key = $TAK) order by direction asc;", $verbose);
        foreach my $direction (@directions) {
            my $fields = "UUID, direction";
            my $values = "$TAK, $direction";
            foreach my $year (2014..2017) {
                print "\t$year: $direction\n" if $verbose;
                my @row = row_from_query($db, "select Date from [$TAK\_$direction] where date like '$year%' order by Date asc limit 1;", $verbose);
                push @row, row_from_query($db, "select Date from [$TAK\_$direction] where date like '$year%' order by Date desc limit 1;", $verbose);
                $fields .= ", Y$year\_start, Y$year\_end";
                $values .= ", '$row[0]', '$row[1]'";
            }
            print "$TAK\_$direction, $fields -> $values\n" if $verbose;
            dbdo ($db, "insert or replace into [SurveyDates] ($fields) Values ($values)", $verbose);
        }
        dbdo($db, "COMMIT", $verbose);
    }
} 
$db->disconnect;

# Subroutines
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
        "SurveyDates" => "UUID Text Primary Key, direction Text, Y2014_start Text, Y2014_end Text, Y2015_start Text, Y2015_end Text, Y2016_start Text, Y2016_end Text, Y2017_start Text, Y2017_end Text",
        );
    foreach my $tablename (%tables) {
        if (exists $tables{$tablename} ) {
            my $result = dbdo($db, "Drop Table if exists [$tablename];", $verbose);
            my $command = "Create Table if not exists [$tablename] ($tables{$tablename})";
            $result = dbdo($db, $command, $verbose);
        }
    }
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

