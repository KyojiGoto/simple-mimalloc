#!/usr/bin/perl

use strict;

# Check for correct usage
if (@ARGV != 2) {
  print "usage: graphbench.pl <dir> <name> \n";
  print "    where <dir> is the directory containing the Results subdir,\n";
  print "    and <name> is the base name of the benchmark.\n";
  die;
}

my $dir=$ARGV[0];
my $benchname = $ARGV[1];

#If dir is not an absolute path, and doesn't start with "." already, add the "./"
if (!($dir =~ /^\// || $dir =~ /^\./)) {
    $dir = "./" . $dir;
}

if (!(-e $dir and -d $dir)) {
  print "usage: graphbench.pl <dir> <name> \n";
  print "    where <dir> is the directory containing the Results subdir,\n";
  print "    and <name> is the base name of the benchmark.\n";
  die;
}

# This common graphing script does not work for larson, since that benchmark
# reports throughput instead of runtime. Check and warn in that case.
if ($benchname eq "larson") {
    print "Warning: larson needs its own graphing script to handle throughput.\n";
    print "Invoking larson's graphtests.pl from its own subdirectory.\n";
    system "$dir/graphtests.pl $dir";
    exit;
}
    
#Initialize from config file
#Each benchmark dir must contain a config.pl file that sets 
#graphtitle for that benchmark. 
my %config;

unless (%config = do "$dir/graph_config.pl") {
            warn "couldn't parse $dir/graph_config.pl: $@" if $@;
            warn "couldn't do $dir/graph_config.pl: $!"    unless %config;
            warn "couldn't run $dir/graph_config.pl"       unless %config;
}

# Initialize list of allocator results to graph.
# uncomment the line corresponding to the allocators you want to graph.
#my @alloclist = ("a2alloc");
#my @alloclist = ("libc", "kheap");
my @alloclist = ("libc");
#my @alloclist = ("libc", "kheap", "a2alloc");
my %names;

# This allows you to give each series a name on the graph
# that is different from the file or directory names used
# to collect the data.  We happen to be using the same names.
$names{"libc"} = "libc";
$names{"kheap"} = "kheap";
$names{"a2alloc"} = "a2alloc";


my $allocator;
my $nthread = 8;


foreach $allocator (@alloclist) {
    open G, "> $dir/Results/$allocator/data";
    for (my $i = 1; $i <= $nthread; $i++) {
	open F, "$dir/Results/$allocator/$benchname-$i";
	my $total = 0;
	my $count = 0;
	my $min = 1e30;
	my $max = -1e30;

	while (<F>) {
	    chop;
	    # Runtime results
	    if (/([0-9]+\.[0-9]+) seconds/) {
		#	 print "$i\t$1\n";
		my $current = $1;
		$total += $1;
		$count++;
		if ($current < $min) {
		    $min = $current;
		}
		if ($current > $max) {
		    $max = $current;
		}
	    }
	}
	if ($count > 0) {
	    my $avg = $total / $count;
	    #	   print G "$i\t$avg\t$min\t$max\n";
	    print G "$i\t$min\n";
	} else {
	    print "oops count is zero, $allocator, $benchname-$i\n";
	}
	close F;
    }
    close G;    

}

my $xrange = $nthread+1;
open PLOT, "|gnuplot";
print PLOT "set terminal pdfcairo\n";
print PLOT "set output \"$dir/$benchname.pdf\"\n";
print PLOT "set title \"$config{graphtitle}\"\n";
print PLOT "set ylabel \"Runtime (seconds)\"\n";
print PLOT "set xlabel \"Number of threads\"\n";
print PLOT "set xrange [0:$xrange]\n";
print PLOT "set yrange [0:*]\n";
print PLOT "plot ";

foreach $allocator (@alloclist) {

    my $titlename = $names{$allocator};
    if ($allocator eq $alloclist[-1]) {
	print PLOT "\"$dir/Results/$allocator/data\" title \"$titlename\" with linespoints\n";
    } else {
	print PLOT "\"$dir/Results/$allocator/data\" title \"$titlename\" with linespoints,";
    }
}
close PLOT;


