use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan tests => 3;

ok my $sth1 = $dbh->prepare("SELECT 1");
ok my $sth2 = $dbh->prepare("SELECT 1", { mariadb_server_prepare => 1 });

# install a handler so that a warning about unfreed resources gets caught
$SIG{__WARN__} = sub { die @_ };

END {
    my $sth1_copy = $sth1;
    my $sth2_copy = $sth2;
    pass if $sth1;
}
