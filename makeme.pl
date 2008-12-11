#!/usr/bin/perl -w

# Connect to the database.
use Alpha::Float;
use Data::Dumper;
use DBI;

# do this
# (11114 * 170) + (11114 / (512 / 170) * (512 % 170)) + 170
# thats 11114 is the offset we get from the cusidx file
# code to skip:
# $size = 512; 
# $rec_size = 170;
# $rec_no = 2552; 
# $recs_per_block = floor($size / $rec_size); 
# $add = $size % $rec; 
# print $rec_no * $rec_size + (floor($rec_no / $recs_per_block) * $add) + $rec_size,"\n"

#die "usage: $0 <cust_num>" unless $ARGV[0];
my $block_size = 512;

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

sub show_user {
    my ($user) = (shift);
    print Dumper $user;
}

my %users = ();
my $sql;

#the x36 is for the Floating point vaules (MTD, YTD, crdlmit) which we don't care about yet...
#my $idx_rec = 21;
my $idx_rec = 11;
my $cus_rec = 170;
my $xtr_rec = 512;
#my $idx_tpl = "A10 A5 H12";
my $idx_tpl = "A5 H12";
my $cus_tpl = "A5 A25 A25 A25 A15 A2 A9 x2 A1 x18 x1 x1 x1 A10 A10";
my $xtr_tpl = "A5 A15 A15 A21 A5 A120 A4 A6 A1 A6 A2 A1 C A10 A30 A5 A5 A5  x255";

my ($rec_blocks, $skip_bytes);
if ($block_size % $idx_rec) {
    $rec_blocks = int($block_size / $idx_rec);
    $skip_bytes = ($block_size) - ($rec_blocks * $idx_rec);
}
print "skip_bytes = $skip_bytes\nrec_blocks = $rec_blocks\n";

#no wod the next
#open (CUSIDX, "/home/efk/parse/cst/cusid4.dat");
open (CUSIDX, "/home/efk/parse/cst/cusidx.dat");
open (CUSMAS, "/home/efk/parse/cst/cusmas.dat");
open (CUSXTR, "/home/efk/parse/cst/cusxtr.dat");
my $offby=0;

my ($idx, $cst, $xtr);

while (read(CUSIDX, $idx, $idx_rec)) {
    my ($cust_num, $offset) = unpack($idx_tpl, $idx);
    if (!(++$offby % $rec_blocks)) { 
        seek(CUSIDX, $skip_bytes, 1); 
    }
    #next unless ($cust_num eq $ARGV[0]);
    next unless ($cust_num =~ /^[0-9]{5}$/);
    $offset = float_conv($offset) -1;
    $users{$cust_num} = {  
            #"name"      => $cus_name,
            "cust_num"	=> $cust_num,
            "offset"    => $offset
    };

    next unless ($offset);
    #seek (CUSMAS, SEEK_SET, get_offset($users{$cust_num}->{$offset}, $cus_rec);
    seek(CUSMAS, get_offset($offset, $cus_rec), 0);
    read(CUSMAS, $cst, $cus_rec);
    my($xxcust_num, $name, $address, $address2, $city, $state, $zip, $custype, $phone, $phone2) = unpack($cus_tpl, $cst);
    #$, = "\n";
    #print unpack($cus_tpl, $cst);

    seek(CUSXTR, get_offset($offset, $xtr_rec), 0);
    read(CUSXTR, $xtr, $xtr_rec);
    my($xcust_num, $resale, $terms, $cc, $cc_exp, $notes, $added, $last_act, $flag, $amt, $renew, $keep_hist, $hist_mos, $fax, $email, $club1, $club2, $club3, $fill) = unpack($xtr_tpl, $xtr);
    #print "\n";
    #$, = "\n";
    #print unpack($xtr_tpl, $xtr);
    #$cst = initcaps($cst);
    $state =~ tr/[a-z]/[A-Z]/;

    my ($l_name, $f_name);
    $l_name = "";
    $f_name = "";
    #$name = initcaps($name) if ($name);
    $name =~ s/(\w+)/\u\L$1/g if ($name);
    $_ = $name;
    #chomp;
    #if (/(\w*)[ ]+(\w*)/) {
    #    $f_name = $1; 
    #    $l_name = $2;
    #    #$name = $l_name . ", " . $f_name;
    #}
    #elsif(/(\w*)[ ]*,[ ]+(\w*)/) {
    #    $f_name = $2;
    #    $l_name = $1;
    #}
    ( $f_name = $2, $l_name = $1 ) if (/^([^,]*),\W*(.*)$/);
    if (!$f_name) {
        ( $f_name = $1, $l_name = $2 ) if (/(.*)\W+(\w*)/);
    }
    #$address = initcaps($address) if ($address);
    $address =~ s/(\w+)/\u\L$1/g if ($address);
    #$address2 = initcaps($address2) if ($address2);
    $address2 =~ s/(\w+)/\u\L$1/g if ($address2);
    #$city = initcaps($city) if ($city);
    $city =~ s/(\w+)/\u\L$1/g if ($city);
    #print "$f_name - $l_name - $name\n" if ($cust_num eq '20704');
    #print "$f_name - $l_name - $name\n" if ($cust_num eq '20704');
    $name =~ s/[ ]*//g;
    $cc =~ tr/0-9//cd;
    $users{$cust_num}->{"cust_num"} = $cust_num;
    $users{$cust_num}->{"name"} = $name;
    $users{$cust_num}->{"f_name"} = $f_name;
    $users{$cust_num}->{"l_name"} = $l_name;
    $users{$cust_num}->{"phone"} = $phone;
    $users{$cust_num}->{"phone2"} = $phone2;
    $users{$cust_num}->{"address"} = $address;
    $users{$cust_num}->{"address2"} = $address2;
    $users{$cust_num}->{"city"} = $city;
    $users{$cust_num}->{"state"} = $state;
    $users{$cust_num}->{"zip"} = $zip;
    $users{$cust_num}->{"cust_num"} = $cust_num;
    $users{$cust_num}->{"cc_num"} = $cc;
    $users{$cust_num}->{"cc_exp"} = $cc_exp;
    $users{$cust_num}->{"notes"} = $notes;
    $users{$cust_num}->{"added"} = $added;
    $users{$cust_num}->{"amt"} = $amt;
    $users{$cust_num}->{"last_act"} = $last_act;
    $users{$cust_num}->{"keep_hist"} = $keep_hist;
    $users{$cust_num}->{"hist_mos"} = $hist_mos;
    $users{$cust_num}->{"fax"} = $fax;
    $users{$cust_num}->{"email"} = $email;
    $users{$cust_num}->{"club1"} = $club1;
    $users{$cust_num}->{"club2"} = $club2;
    $users{$cust_num}->{"club3"} = $club3;

}
#show_user($users{$ARGV[0]});

# close all files
close (CUSIDX);
close (CUSMAS);
close (CUSXTR);
my $dbh = DBI->connect("DBI:mysql:database=winelibrary;host=db","user", "pass",{'RaiseError' => 1 }); 
#$dbh->do("delete from users;");
@use_list = (
    'cust_num',
    'f_name',
    'l_name',
    'name',
    'phone',
    'phone2',
    'address',
    'address2',
    'city',
    'amt',
    'state',
    'zip',
    'cc_num',
    'cc_exp',
    'notes',
    'added',
    'last_act',
    'keep_hist',
    'hist_mos',
    'fax',
    'email',
    'club1',
    'club2',
    'club3'
);
$s = "REPLACE INTO users (";
$d = " VALUES (";
#$sth = $dbh->prepare("SELECT * FROM shipping_addresses WHERE shipping_name = \"ICS Address\" and cust_num = ? AND ship_1 = ? and ship_2 = ? and ship_3 = ? and ship_4 = ?;");
$sth = $dbh->prepare("SELECT * FROM shipping_addresses WHERE shipping_name = \"Billing Address\" and cust_num = ?;");
$sth1 = $dbh->prepare("UPDATE shipping_addresses SET shipping_name = \"Billing Address\", ship_1 = ?, ship_2 = ?, ship_3 = ?, ship_4 = ? where cust_num = ? AND shipping_name = \"Billing Address\";");
$sth2 = $dbh->prepare("INSERT IGNORE INTO shipping_addresses (cust_num, shipping_name, ship_1, ship_2, ship_3, ship_4) VALUES (?, \"Billing Address\",  ?, ?, ?, ?);");
$sth_dirty = $dbh->prepare("select cust_num, dirty from users where cust_num = ? and dirty > 0;");
$sth_emails = $dbh->prepare(qq{select cust_num, email from emails where email = ?;});
foreach $user (keys %users) {
    $t_s = $s;
    $t_d = $d;
    #show_user($users{$user});
    foreach $p (@use_list) {
        if ($users{$user}->{$p}) {
            $t_s .= "$p, ";
            $val = $users{$user}->{$p};
            $val =~ s/\\//g;
            $val =~ s/;//g;
            $val =~ s/"/'/g;
            $users{$user}->{$p} = $val;
            $t_d .= "\"$val\", ";
        }
    }
    $t_s = substr($t_s, 0, -2);
    $t_d = substr($t_d, 0, -2);
    $t_s .= ")";
    $t_d .= ")";
    #print "$t_s . $t_d\n";
    if ($t_s =~ /cust_num/) {
        #print "\$ship_1 should be " . $users{$user}->{'name'} . "\n";
        my ($cust_num, $ship_1, $ship_2, $ship_3, $ship_4) = (
            $users{$user}->{'cust_num'},
            $users{$user}->{'address'}, 
            $users{$user}->{'address2'}, 
            $users{$user}->{'name'}, 
            $users{$user}->{'city'} . ", " . $users{$user}->{'state'} . " " . $users{$user}->{'zip'} 
        );
        #print "\$ship_1 IS " . $ship_1 . "\n";
        if ($cust_num =~ /[0-9]{3,6}/) {
            $sth_dirty->execute($cust_num);
            if ($ship_1) {
                $ship_2 = "" unless $ship_2;
                $ship_3 = "" unless $ship_3;
                $ship_4 = "" unless $ship_4;
                $sth->execute($cust_num);
                if ($sth->rows()) {
                    #print "In the UPDATE - $ship_1, $ship_2, $ship_3, $cust_num\n";
                    if ($sth_dirty->rows() == 1) {
                        #no
                    }
                    else {
                        $sth1->execute($ship_1, $ship_2, $ship_3, $ship_4, $cust_num);
                    }
                }
                else {
                    #print "In the INSERT IGNORE\n";
                    $sth2->execute($cust_num . "", $ship_1, $ship_2, $ship_3, $ship_4); 
                }
            }
            $sql = $t_s . $t_d . ";";
            #print "$sql\n";
            #print "\n\nHead\n$sql" if ($user eq '20704');
            #print "\n\nWalkush\n$sql" if ($user eq '31246');
            #print "\n\n???\n$sql" if ($user eq '31245');
            if ($sth_dirty->rows() == 1) {
                print "DIrty on $cust_num\n";
                #dirty, do nothing
            } 
            else {
                $dbh->do($sql);
            }
        }
        #44874
        my $em = $users{$user}->{'email'};
        my $cus = $users{$user}->{'cust_num'};
        my $bob = ($cus + 1) -1;
        if ($em =~ /.*\@.*/) {
            #$sql = "delete from users_emails where cust_num = \"$bob\";";
            #$sql = "delete from users_emails where email = \"$em\";";
            #$dbh->do($sql);
            $sth_emails->execute($em);
            #print $sth_emails->rows() . "\n" if ($cus eq "44874");
            if ($sth_emails->rows()) { #the email is IN the system
                my ($db_cust, $email2) = $sth_emails->fetchrow();
                if ($db_cust) {
                    if ($db_cust eq $cus) {
                        # then we're fine....
                    }
                    else {
                        if ($db_cust =~ /^-/) { #negative customer, just update it
                            $sql = "update emails set cust_num = $cus where email = \"$em\";";
                            $dbh->do($sql);
                        }
                        #print "Error... $db_cust - $cus\n";
                        #dunno what do to
                    }
                }
                else {
                    $sql = "update emails set cust_num = $cus where email = \"$em\";";
                    $dbh->do($sql);
                }
            }
            else {
                #THIS IS REALLY BROKEN>... IT ALLOWS MULTIPLES if the email in alpha is screwey (i.e. b/c the field is too short)
                #nothing in emails
                #add it
                print "Adding email $em\n";
                $sql = "insert ignore into emails (cust_num, email) VALUES (\"$users{$user}->{'cust_num'}\", \"$users{$user}->{'email'}\");";
                #$sql = "delete from emails where email = \"$em\" and w3h = 0 and m3h = 1;";
                $dbh->do($sql);
            }
        }
    }
}
