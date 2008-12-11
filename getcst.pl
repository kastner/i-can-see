#!/usr/bin/perl
#system("./runcst");
#system("rm *.cst");
unlink("cusxtr.dat");
unlink("cusmas.dat");
unlink("cusidx.dat");
system("rm *.dat.[0-9]*");
#system("rm cusxtr.dat");
#system("rm cusmas.dat");
#system("rm cusidx.dat");
#system("wget ftp://root:c0ntr0l\@ics.winelibrary.com/dsk0/nss/*");
system("wget ftp://root:c0ntr0l\@ics/dsk0/031001/cusxtr.dat");
system("wget ftp://root:c0ntr0l\@ics/dsk0/031001/cusmas.dat");
system("wget ftp://root:c0ntr0l\@ics/dsk0/031001/cusidx.dat");
system("./makeme.pl");
