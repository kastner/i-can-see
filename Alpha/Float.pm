#	Package Alpha/Float.pm ... this has the float functions to convert
#	alpha stuff to normal numbers.
package Alpha::Float;
use strict;
use warnings;

BEGIN {
	use Exporter   ();
	use Math::BigFloat;
	our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

	# set the version for version checking
	$VERSION     = 0.01;

	@ISA         = qw(Exporter);
	@EXPORT      = qw(&float_conv &float_conv_old &initcaps);

	# your exported package globals go here,
	# as well as any optionally exported functions
	@EXPORT_OK   = qw(&float_conv &float_conv_old &initcaps);
}
our @EXPORT_OK;

sub initcaps {
    my %specials;
    $specials{"st"} = "st";
    $specials{"st."} = "st.";
    $specials{"street"} = "street";
    $specials{"ave"} = "ave";
    $specials{"ave."} = "ave.";
    my($out,$in,$word,$lcw,$found);
    $in = $_[0];
    #$in = tr/\./\\\./;
    #$in = lc $in;
    $out='';
    while (length($in) > 0) {
        $in =~ s/^(\W*)([^\W]*)//;
        $out .= $1;         # transfer the white space before words
        $word = $2;         # setup the next word
        ($lcw=$word) =~ tr/A-Z/a-z/;    # make a lower-case version
        $found = $specials{$lcw};   # is the word on the special list?
        if ($found && $out !~ /^ *$/) { # exception word but not first in line
            $word = $found;     # exception words as specified.
        } elsif ($word =~ /^[a-z]*$/i){ # word is only alphabetics?
            $word =~ tr/a-z/A-Z/;   # make an upper-case version
            if ($word && $lcw) {
                $word = substr($word,0,1) . substr($lcw,1,999); # combine lc version
            }
        }
    $out .= $word;
    } $out;
}

sub float_conv_old {
    my $hex_str = shift(@_);

    $_ = $hex_str;

    my $d;
    # re-oder string... Alpha is "middle endian"
    $d .= sprintf "%0.8b%0.8b", hex($2), hex($1) while /(..)(..)/g;
    my @bits = split(//, $d);
    push(@bits, (0) x (48 - @bits));

    my($s, $x, $a, $b, $c);
    foreach my $tuple (
        [\$s,   $bits[0]     ],
        [\$x,   @bits[1..8]  ],
        [\$a,   @bits[9..15] ],
        [\$b,   @bits[16..31]],
        [\$c,   @bits[32..47]],
    ) {
        my($var, @bits) = @$tuple;

        my $i = 0;
        foreach my $bit (reverse @bits) {
            $$var += $bit * 2 ** $i;
            $i++;
        }
    }

    return 0 if ($a == 0 and $b == 0 and $x == 0);
    my $new_x = $x - 128;
    #print "$a, $b, $c, ex = $x -- "; #debuging code, for when I forget how this works

    my $result   = new Math::BigFloat 
    my $mantissa = new Math::BigFloat 0;
    $mantissa += 1;
    $mantissa += $a * 2 ** -7;
    $mantissa += $b * 2 ** -23;
    $mantissa += $c * 2 ** -39;

    #print "mant = $mantissa -- "; #see above

    $result   += $s ? -1 : 1;
    $result   *= $mantissa;
    $result   *= 2 ** ($x - 129);

    return $result;
}

use Inline C => <<'END_C';

#include <stdlib.h>
#include <math.h>

double float_conv(char * bits) {
    char * pEnd;
    int sign, exp;
    long long num;
    long long a, b;
    double mant;
    num = strtoll(bits, &pEnd, 16);
    a = num & 0x00ff00ff00ff;
    b = num & 0xff00ff00ff00;
    a <<= 8;
    b >>= 8;
    num = a | b;
    sign =   num & 0x800000000000;
    exp  = ((num & 0x7f8000000000) >> 39) - 129;
    mant = (double) ((num & 0x007fffffffff) | 0x008000000000);
    mant = (double) (mant * pow(2, exp - 39));
    //return ((exp && num) ? (mant * (sign ? -1 : 1)) : 0);
    if (exp == -129 && num == 0) { return 0; }
    return (mant * (sign ? -1 : 1));
}

END_C

1;

