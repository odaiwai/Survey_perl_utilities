#!/usr/bin/perl
use strict;
use warnings;

my ($TAK, $dir) = (@ARGV);

my %dirs = ("8003"=>"8085",	"8003"=>"8085",	"8004"=>"8124",	"8005"=>"8084",	"8006"=>"8083",	"8083"=>"8006",	"8084"=>"8005",	"8085"=>"8003",	"8124"=>"8004");

my $revdir = $dirs{$dir};
my $basedir = "/home/odaiwai/Documents/Transport_Planning/20170904_sydney_westconnex/08_survey_data/Motorway_data";
my @file1 = `sqlite3 $basedir/traffic_counts.sqlite -header -csv "select * from  [$TAK\_$dir];"`;
my @file2 = `sqlite3 $basedir/traffic_counts.sqlite -header -csv "select * from  [$TAK\_$revdir];"`;
my @daily1 = `sqlite3 $basedir/traffic_counts.sqlite -header -csv "select time, sum(volume)/count(volume) from traffic_counts where traffic_asset_key='$TAK' and direction='$dir' and weekday in (2, 3, 4, 5, 6) group by time;"`;
my @daily2 = `sqlite3 $basedir/traffic_counts.sqlite -header -csv "select time, sum(volume)/count(volume) from traffic_counts where traffic_asset_key='$TAK' and direction='$revdir' and weekday in (2, 3, 4, 5, 6) group by time;"`;

print_columns(\@file1, \@file2, "0, 0, 0, 0, 0, 0, 0, , ");
print_columns(\@daily1, \@daily2, "0, 0, , ");

## subs
sub print_columns {
    my $list1ref = shift;
    my $list2ref = shift;
    my @list1 = @{$list1ref};
    my @list2 = @{$list2ref};
    my $separator = shift;
    my $index = 0;
    my ($list1, $list2) = (1,1);
    while ($list1 or $list2) {
        if (defined($list1[$index])) {
            my $line = $list1[$index];
            chomp $line;
            print $line . ", , ";
        } else {
            $list1 = 0;
            print $separator;
        }
        if (defined($list2[$index])) {
            my$line = $list2[$index];
            chomp $line;
            print $line;
        } else {
            $list2 = 0;
        }
        print "\n";
        $index++;
    }
    my $end = @file1;
    if (@file2 > $end) {$end = @file2;}
    return 1;
}