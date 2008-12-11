#!/usr/bin/perl -w
#
use Data::Dumper;
use POSIX qw(strtod);
use Alpha::Float;
die "Use ./find.pl <file> <offset> <record_size> <template> <column> <value>\n" unless @ARGV;
$block_size = 512;
$master = $ARGV[0];
$jump = $ARGV[1];
$rec_size = $ARGV[2];
$tpl = $ARGV[3];
open (MASTER, $master);
seek(MASTER, $jump, 1);
if ($block_size % $rec_size) {
    $rec_blocks = int($block_size / $rec_size);
    $skip_bytes = ($block_size) - ($rec_blocks * $rec_size);
}
else {
    $rec_blocks = $block_size / $rec_size;
    $skip_bytes = 0;
}
#print "skip_bytes = $skip_bytes\nrec_blockes = $rec_blocks\n";
$i = 0;
print "Searching column $ARGV[4] for $ARGV[5]\n";
while (read(MASTER, $cst, $rec_size)) {
    #print "Reading $rec_size ", tell(MASTER), "\n";
    @bob = unpack($tpl, $cst);
    if ($bob[$ARGV[4]] =~ /$ARGV[5]/) {
        for ($x = 0; $x < scalar @bob; $x++) {
            $bob[$x] = float_conv($bob[$x]) if ($bob[$x] =~ /^[a-f0-9]{6}$/);
            $bob[$x] = float_conv($bob[$x]) if ($bob[$x] =~ /^[a-f0-9]{12}$/);
            print $bob[$x] . "||";
        }
        print " - ", tell(MASTER) - $rec_size, ".", $i;
        print "\n---------------------------------------------------\n";
    } else {
        #print $bob[$ARGV[4]], "\n";
        #$, = "\n";
        #print @bob;
        #print "\n\n";
    }
    #print Dumper unpack($tpl, $cst);
    if (!(++$i % $rec_blocks)) { 
        #print "Skipping $skip_bytes\n"; 
        seek(MASTER, $skip_bytes, 1); 
    }
    #print "\n\n";
}
close (MASTER);
