#!/usr/bin/perl -w

use warnings;
use POSIX;

my $qmail = "| /var/qmail/bin/qmail-inject ";

my $msg;

my $edir = "/var/www/find/emails/";
my $file;
$file = $edir . strftime("%m%d%Y", localtime());
open (EMAILFILE, "$file") or die ("Can't open $file $!\n");
print "Da Email = $file\n";

while (<EMAILFILE>) {
    $msg .= $_;
}

close(EMAILFILE);

$msg =~ s/\r\n/\n/g;

my @emails = ("efk\@winelibrary.com");
#print $msg;
 
for my $email (@emails) {
    $email =~ s/\s//g;
    #remove whitespace to avoid bounces
    $email =~ s/^.*<([^>]*)>?.*$/$1/;
    #escape long format
    $email =~ s/^(.*);.*$/$1/;
    #get first of multiples
    ##$email =~ s/'//g;
    ##$email =~ s/`//g;
    #print "email is $email\n";
    my $emsg = $msg;
    $emsg =~ s/{email}/$email/g;
    my $verp = $email;
    $verp =~ s/@/=/g;
    $emsg =~ s/{verp}/$verp/g;
    open (QI, $qmail . "$email") or die $!;
    print QI $emsg;

    close QI;
}
