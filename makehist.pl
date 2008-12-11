#!/usr/bin/perl -w
#Program to import all the customer history
#we *can* do an offset type thing - so we can not read the whole thing
#that may be part of the module I want to make
#the module should also do the recordsize stuff.

use Data::Dumper;
use DBI();
use POSIX qw(strtod);
use Alpha::Float;

$block_size = 512;
#$master = $ARGV[0];
$master = "cphmas.dat";
#$jump = $ARGV[1];
$jumper = `cat jumper`;

unlink <o*.dat>;
my $mark_it = 0;
my %orders;
if ($jumper) {
    ($jump, $offby) = split(/\./,$jumper);
    print "$jump - $offby\n";
}
else {
    $offby=0;
    $jump=0;
}
print "Jump is now $jump, $offby\n";
$rec_size = 102;
$tpl = "A5 A8 A6 A5 B2 B1 A1 A5 A3 A5 A1 A1";
$tpl = "A5 a8 a6 a5 s A1 l C a3 l C a1 a1";
open (MASTER, $master);
seek(MASTER, $jump, 1);
if ($block_size % $rec_size) {
    $rec_blocks = int($block_size / $rec_size);
    $skip_bytes = ($block_size) - ($rec_blocks * $rec_size);
}
else {
    $rec_blocks = 1;
    $skip_bytes = 0;
}
print "skip_bytes = $skip_bytes\nrec_blockes = $rec_blocks\n";
my $dbh = DBI->connect("DBI:mysql:database=winelibrary;host=db","user", "pass",{'RaiseError' => 1 }); 
#open SQL, ">sql" or die;

my $hist_daily_del = $dbh->prepare("DELETE FROM hist_daily where order_num = ?;");
$rec_size = 102;
$cst = "\0" x $rec_size;
while (read(MASTER, $cst, $rec_size)) {
    #print "Reading $rec_size\n";
    my($cust_num, $hist_date, $order_num, $item_num, $qty, $uom, $retail, $retail2,  $pct_disc, $cost, $cost2, $sale_price) = unpack($tpl, $cst);
    die("AGGGGGGGGGGGGGGGGGG") if ($hist_date =~ /200[34]-/);
    $cst = "\0" x $rec_size;
    if (!(++$offby % $rec_blocks)) { 
        print "Skipping $skip_bytes - $hist_date/$order_num/$item_num/$cust_num\n" if ($cust_num !~ /^\[/); 
        seek(MASTER, $skip_bytes, 1); 
    }
    if ($mark_it) {
        if ($offby > 233736) {
            $mark_it=0;
            $spot = tell(MASTER);
            $offby_keep = $offby;
        }
    }
    #print "\n\n";
    if ($item_num =~ /[0-9]{5}/) {
        #print tell(MASTER), " $cust_num | $hist_date | $order_num | $item_num | $qty\n";
        #$order_num =~ tr/[A-Z]//d;
        $mark_it = 1;
        $dbh->do("DELETE FROM new_orders where order_num=\"$order_num\";");
        #$dbh->do("DELETE FROM hist where order_num=\"$order_num\" AND cust_num=\"$cust_num\";");
        #$dbh->do("DELETE FROM other_order_table where order_num=\"$order_num\";");
        $dbh->do("DELETE FROM hist where order_num=\"$order_num\" AND item_num = \"$item_num\" AND cust_num = \"$cust_num\" AND qty = \"$qty\";");
        $sql = "INSERT INTO hist (cust_num, hist_date, order_num, item_num, qty, uom, retail, retail2, pct_disc, cost, cost2, sale_price) VALUES ('$cust_num', '$hist_date', '$order_num', $item_num, '$qty', '$uom',$retail,'$retail2', '$pct_disc', '$cost', '$cost2', '$sale_price');";
        #print "$sql\n" if ($order_num =~ /O41074/);
        $dbh->do($sql);
        $hist_daily_del->execute($order_num);
        if ($order_num =~ /O[0-9]{5}/) {
            if (!$orders{$order_num}) {
                $sql = "UPDATE orders set status = \"NG\", cust_num=\"$cust_num\" where order_num = \"$order_num\";";
                $dbh->do($sql);
                #$orders{$order_num} = $dbh->do($sql);
                $orders{$order_num} = 1;
            }
        }
    }
}
#close SQL;
close MASTER;
if ($spot) {
    $writelin = "$spot.$offby_keep";
    system("echo $writelin > jumper");
}
$dbh->disconnect();
#die ("No user to search for") unless $ARGV[0];

