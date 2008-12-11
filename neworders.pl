#!/usr/bin/perl -w

use strict;
use DBI;
use Net::FTP;
use Math::BigFloat;
use Alpha::Float;
use Date::Parse;
use POSIX qw(strftime);

my $dbh = DBI->connect("DBI:mysql:database=winelibrary;host=db","user", "pass",{'RaiseError' => 1 }); 

sub get_offset {
    my ($rec_no) = (shift);
    my ($rec_size) = (shift);
    my ($recs_per_block, $add_bytes);
    my $block_size = 512;
    my $res;
    if ($block_size % $rec_size) {
        $recs_per_block = int($block_size / $rec_size);
        $add_bytes = $block_size % $rec_size;
    }
    else {
        $recs_per_block = $block_size / $rec_size;
        $add_bytes = 0;
    }
    $res = ($rec_no * $rec_size + (int($rec_no / $recs_per_block) * $add_bytes));
    #print "\n$rec_no = $res\n";
    return($res);
}

sub make_money {
    my $money_str = shift(@_);
    $money_str /= 100;
    return sprintf("%.2f",$money_str);
}

my $block_size = 512;
my $header_size = 73;
my $body_size = 73;
my $ship_size = 512;

#header def from cst001.bsi
#order num, rectype, pik, cust_num, status (Q, I, D), Taxable, order_date;
#my $header_tpl = "x6 x1 x1 A5 A1 A1 A6 x6 x6 H12 H12 x x x x15 A1";
my $header_tpl = "x6 x1 x1 A5 A1 A1 A6 x6 x6 H12 H12 x20 A1";
#my $header_tpl = "A6 x1 x1 A5 A1 x1 A6 x6 x6 H12 H12 v v A1 A15 A1";
my $body_tpl = "a6   x1  a5   H12 H12 H12 H12 H12 a1 H12 H12 H12 H12";
              #order   #item 
my $ship_tpl = "A6 A100 A10 A10 A120 A2 x2 A6 A21 A5";
my $ship_tpl_short = "A6 x244 A6";

my $body;
my $pu = $dbh->prepare("INSERT IGNORE INTO pickup (order_num, cust_num) VALUES (?, ?);");
my $hts = $dbh->prepare("INSERT IGNORE INTO hts (order_num, added) VALUES (?, NOW());");
my $hist_daily_del = $dbh->prepare("DELETE FROM hist_daily where order_num = ?;");
my $new_del = $dbh->prepare("DELETE FROM new_orders where order_num = ?;");
my $hist_daily = $dbh->prepare("INSERT INTO hist_daily (cust_num, hist_date, order_num, item_num, qty, retail, uom, pct_disc, cost, retail2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0);");
my $order_find = $dbh->prepare("select * from orders where order_num  = ?;");
my $order_add = $dbh->prepare("INSERT INTO orders (cust_num, order_num, status, ng_date, date_added, entered_by, ship_to, notes, cc_num, cc_exp, pay_type, phone, p_name, s_total, tax, event_date) VALUES (?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
my $order_update = $dbh->prepare("update orders set cust_num = ?, status = ?, event_date = ?, ng_date = ?, entered_by = ?, ship_to = ?, notes = ?, cc_num = ?, cc_exp = ?, pay_type = ?, phone = ?, p_name = ?, s_total = ?, tax = ? WHERE order_num = ?;");
my $hist_find = $dbh->prepare("select * from hist where order_num = ?;");
my $item_find = $dbh->prepare("select scode from items.items where item = ?;");
my $is_find = $dbh->prepare("select * from items.items_sis where item = ?;");
my $is_add = $dbh->prepare("insert into items.items_sis (item, last_sold_on) VALUES (?, ?);");
my $is_update = $dbh->prepare("update items.items_sis set last_sold_on = ? where item = ?;");
my $oc_find = $dbh->prepare(qq{select * from orders_coupons where order_num = ? and coupon_code = ?;});
my $oc_add = $dbh->prepare(qq{INSERT INTO orders_coupons (order_num, coupon_code) VALUES (?, ?);});

my ($header, $ship_rec, $sql, $flag, $skip_bytes, $rec_blocks, $sth, $g_num);
if ($block_size % $body_size) {
    $rec_blocks = int($block_size / $body_size);
    $skip_bytes = ($block_size) - ($rec_blocks * $body_size);
    print "skip_bytes_body = $skip_bytes\nrec_blockes_body = $rec_blocks\n";
}
#$sth = $dbh->prepare("SELECT MAX(order_num) as moi from hist;");
#$sth = $dbh->prepare(qq{select max(order_num) from hist where to_days(NOW()) - to_days(hist_date) <=3;});
#$sth->execute || die "Error fetching data: $DBI::errstr\n";;
#my $num = $sth->fetchrow_array;
#----UNCOMMENT----
#$num =~ s/O(\d+)/"O" . ($1 - 5000)/e;
#$num =~ s/O(\d+)/"O" . ($1 - 300)/e;
my $num = "O24000";
$num = ($ARGV[0]) ? $ARGV[0] : "O47698";
my $ng_target = strftime("%m%d%y", localtime(time - (86400 * 4)));
print "num is set to $num\n";
#print "Target ng_date set to $ng_target\n";
#my $tn = $1 - 50000;
#my $jum = get_offset($tn, $ship_size);
#print "$jum - $tn... Jump\n";
#$num = "O10001";
#$num = "O35850";
##my $ship_file = "shipto.dat";
#seek to the pos in the shipto.
#my $ftp = Net::FTP->new("ics.winelibrary.com");
my $ftp = Net::FTP->new("ics");
$ftp->login("root","c0ntr0l") or die("could not log in $!\n");
$ftp->cwd("/dsk0/031001/");
##unlink $ship_file;
##die ("Can't get $ship_file $!\n") unless $ftp->get($ship_file);
##rint "done with shipto\n";
my $valid_days = 5;
open SHIPTO, "shipto.old";
#seek(SHIPTO, 15360000,1);
seek(SHIPTO, 0,1);
my $o = 0;
$dbh->do("delete from new_orders where order_date = curdate();"); 
while (read(SHIPTO, $ship_rec, $ship_size)) {
    my($order_num, $ng_date) = unpack($ship_tpl_short, $ship_rec);
    if ($flag) {
        my($order_num, $ship_to, $phone, $p_name, $notes, $entered_by, $ng_date, $cc_num, $cc_exp) = unpack($ship_tpl, $ship_rec);
        if ($order_num eq "[[[[[[") {
            print "Lost flag - $order_num, $ship_to, $entered_by\n";
            $flag = 0;
            next; 
        }
        if ($ng_date =~ m!([0-9]{2})([0-9]{2})([0-9]{2})!) {
            if (time - str2time("$1/$2/$3") > (86400 * $valid_days)) {
                #print "time - str2time($1/$2/$3) = " . (time - str2time("$1/$2/$3")), "\n";
                print "Lost flag - $ng_date, $order_num, $ship_to, $entered_by\n";
                #exit();
                $flag = 0;
                print "UN setting flag...\n";
                next;
            }
        }
        next unless ($order_num =~ /[A-Z][0-9]{4,6}/);
        $o++;
        print "$order_num - $ng_date - $p_name - $o\n";
        print "$order_num, $ship_to\n" if ($order_num eq "O64645");
        print "$order_num, $ship_to\n" if ($order_num eq "O64115");
        print "Found order 52401...\n" if ($order_num =~ "/52401/");
        next if ($order_num =~ /87423/);
        next if ($order_num =~ /88020/);
        print "$order_num, $cc_num\n" if ($order_num eq "O98700");
        #print "$order_num\n";
        #get the file
        my $g_num = lc($order_num);
        unlink($g_num . ".dat");    # delete the file
        $hist_find->execute($order_num);  #check for history
        $order_find->execute($order_num);
        my $stt = "";
        if ($order_find->rows()) {
            $stt = $order_find->fetchrow_hashref->{ship_to};
            print "$order_num $stt\n" if ($order_num eq "O66304");
        }
        while ($notes =~ m!WC:([a-zA-Z0-9_-]*)!mgis) {
            $oc_find->execute($order_num, $1);
            if (!$oc_find->rows()) {
                $oc_add->execute($order_num, $1);
            }
        }
        $stt = 1 if ($order_num eq "O24231");

        $o++;
        #print "$o\n" if ($o % 100 == 0);
        if (!$hist_find->rows() or $stt eq "") {
            if ($ftp->get($g_num . ".dat")) { # d/l the file
                $ftp->get($g_num . ".dat") if (!-e "$g_num.dat");
                open ORDER, "$g_num.dat" or die "Can't get ORDER file $g_num.dat\n";
                read ORDER, $header, $header_size;
                my ($cust_num, $status, $taxable, $order_date, $s_total, $tax_total, $pay_type) = unpack($header_tpl, $header);
                if ($cust_num =~ /[0-9]{4,6}/) { # valid cust
                    $order_date =~ s/(.*)([0-9]{2}$)/20$2$1/;
                    $ng_date =~ s/(.*)([0-9]{2}$)/20$2$1/;
                    my $today = strftime "%Y%m%d", localtime;
                    $taxable = ($taxable) ? "Taxable" : "Non-taxable";
                    $s_total = float_conv($s_total);
                    $tax_total = float_conv($tax_total);
                    $new_del->execute($order_num);
                    print "$order_num, $status, $cust_num, $ship_to - $o\n" if ($order_num eq "O66304");
                    if ($order_num eq "O52401") {
                        print "$order_num, $status, $cust_num\n";
                    }
                    if ($order_num eq "O66304") {
                        print "$order_num, $status, $cust_num\n";
                    }
                    #$sql = "INSERT INTO new_orders (cust_num, order_num, order_date, status, taxable) VALUES ($cust_num, \"$order_num\", \"$order_date\", \"$status\", \"$taxable\");";
                    #print "$sql \n";
                    #$dbh->do($sql, undef, $order_num);
                    #$sql = "DELETE FROM orders where order_num = \"$order_num\";";
                    #$dbh->do($sql);
                    SWITCH: {
                        $status = "NG", last SWITCH if $status =~ /I/;
                        $status = "NC", last SWITCH if $status =~ /D/;
                        $status = "NB", last SWITCH if $status =~ /Q/;
                        $status = "NA";
                    }
                    $order_find->execute($order_num); 
                    if ($order_find->rows()) {
                        print "updating $cust_num, $status, $ng_date, $entered_by, $ship_to, $notes, $cc_num, $cc_exp, $phone, $p_name, $s_total, $tax_total, $order_num\n" if ($order_num eq "O66304");
                        #print "Update Order $order_num\n";
                        $order_update->execute($cust_num, $status, $order_date, $ng_date, $entered_by, $ship_to, $notes, $cc_num, $cc_exp, $pay_type, $phone, $p_name, $s_total, $tax_total, $order_num);
                    }
                    else {
                        #print "Adding order $order_num\n";
                        $order_add->execute($cust_num, $order_num, $status, $ng_date, $entered_by, $ship_to, $notes, $cc_num, $cc_exp, $pay_type, $phone, $p_name, $s_total, $tax_total, $order_date);
                    }
                    if ($ship_to =~ m!PI(CK|KC)\s?UP|P[/-]U!i) {
                        #print "Adding $order_num to pickup \n";
                        $pu->execute($order_num, $cust_num);
                    }
                    if ($ship_to =~ m!HOLD.*TO.*SHIP!i) {
                        #print "Adding $order_num to hts \n";
                        $hts->execute($order_num);
                    }
                    $sth = $dbh->prepare("SELECT order_num, vfc_id from orders_vfc where order_num = \"$order_num\";");
                    $sth->execute || die "Error fetching data: $DBI::errstr\n";;
                    if(!$sth->fetchrow_array) {
                        #print "Did not Found $order_num in orders_vfc\n";
                        $dbh->do("REPLACE INTO orders_vfc (order_num, vfc_id) VALUES (\"$order_num\", 0);");
                    }
                    # Item information for daily orders.
                    my $blocker = 1;
                    $hist_daily_del->execute($order_num);
                    while (read(ORDER, $body, $body_size)) {
                        my ($order_no, $item_no, $case_qty, $bottle_qty, $case_return, $bottle_return, $retail, $rtl, $disc, $cost) = unpack($body_tpl, $body);
                        last if ($item_no =~ /\]\]\]/);
                        next if ($order_no =~ /\]\]\]/);
                        my $uom = ($rtl eq "U") ? "B" : "D";
                        $retail = float_conv($retail);
                        $disc = float_conv($disc);
                        $case_qty = float_conv($case_qty);
                        $bottle_qty = float_conv($bottle_qty);
                        $item_find->execute($item_no);
                        $cost = float_conv($cost);
                        my $scode = $item_find->fetchrow_array; 
                        if (!$scode) {
                            $o = 11111111;
                            #last();
                            #print STDERR "No scode for $item_no - $order_no\n";
                            #$scode = 1;
                        }
                        else {
                            my $qty = $case_qty * $scode + $bottle_qty;
                            $bottle_return = float_conv($bottle_return);
                            $case_return = float_conv($case_return);
                            $bottle_return = 0 unless ($bottle_return);
                            $case_return = 0 unless ($case_return);
                            my $qt_ret = $case_return * $scode + $bottle_return;
                            $qty -= $qt_ret;
                            if (($bottle_return != $bottle_qty) || ($case_return != $case_qty)) {
                                #print "$cust_num, $order_num, $item_no, $qty\n";
                            print "$cust_num, $order_num, $item_no, $qty\n" if ($order_num eq "O04546");
                                $hist_daily->execute($cust_num, $order_date, $order_num, $item_no, $qty, $retail, $uom, $disc, $cost);
                                $is_find->execute($item_no);
                                if ($is_find->rows()) {
                                    $is_update->execute($order_date, $item_no);
                                }
                                else {
                                    $is_add->execute($item_no, $order_date);
                                }
                            } # /bottle return
                        }
                        if (!(++$blocker % $rec_blocks)) {
                            seek(ORDER, $skip_bytes, 1);
                        } # /blocker
                        if ($order_num eq "O52390") {
                            print "for order $order_num ... $item_no - $case_qty+$bottle_qty ... -[$case_return+$bottle_return]\n";
                        }
                    } # /while
                } # /valid cust
                else {
                    print "$cust_num\n";
                } # /valid cust
                close ORDER;
            } # /get
            #unlink "$g_num.dat";
        } # /hist
        else {
            # print "Has hist for order $order_num\n";
            $new_del->execute($order_num);
            $hist_daily_del->execute($order_num);
        } # /hist-else
    } # /flag
    elsif (!$flag) {
        $flag = ($order_num eq $num);   # found the last valid order number
        seek(SHIPTO, -$ship_size, 1) if ($flag);
        #print "Checking $ng_date vs $ng_target\n";
        #$flag = ($ng_date eq $ng_target);
        #if ($flag) { print "set flag - $order_num\n";  }
    } # /flag-else
} # /while
$ftp->close();
$ftp->quit;
#$sth->finish();
$pu->finish();
$hist_daily_del->finish();
$hist_daily->finish();
$new_del->finish();
$oc_add->finish();
$oc_find->finish();
$hist_find->finish();
$item_find->finish();
$order_add->finish();
$order_update->finish();
$order_find->finish();
$is_find->finish();
$is_add->finish();
$is_update->finish();
$hts->finish();
#$dbh->disconnect();
