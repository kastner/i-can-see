#!/usr/bin/perl
system("rm cphmas.dat");
system("wget ftp://host/dsk0/031001/cphmas.dat");
system("./makehist.pl");
