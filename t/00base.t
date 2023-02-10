use strict;
use warnings;

use Test::More tests => 7;

# Base DBD Driver Test
BEGIN {
    my $tb = Test::More->builder;
    my $fo = $tb->failure_output;
    my $error = '';
    $tb->failure_output(\$error);
    use_ok('DBI') or do {
        $error =~ s/^.*? Error:\s*//s;
        $error =~ s/\n(?:# ?)?/ /g;
        BAIL_OUT "Unable to load DBI: $error";
    };
    $error = '';
    use_ok('DBD::MariaDB') or do {
        $error =~ s/^.*? Error:\s*//s;
        $error =~ s/\n(?:# ?)?/ /g;
        BAIL_OUT "Unable to load DBD::MariaDB: $error";
    };
    $tb->failure_output($fo);
}

diag "Perl version is " . ($] >= 5.010 ? $^V : $]);

ok eval { DBI->VERSION(1.608) }, "DBI version is OK";
diag "DBI version is $DBI::VERSION";

my $switch = DBI->internal;
cmp_ok ref $switch, 'eq', 'DBI::dr', 'Internal set';

# This is a special case. install_driver should not normally be used.
my $drh= DBI->install_driver('MariaDB');

ok $drh, 'Install driver';

cmp_ok ref $drh, 'eq', 'DBI::dr', 'DBI::dr set';

ok $drh->{Version}, "Driver version is OK";
diag "Driver version is $drh->{Version}";
