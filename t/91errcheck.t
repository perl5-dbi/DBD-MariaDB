use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });

plan tests => 2;

$dbh->do( 'this should die' );
ok $DBI::err, 'error number should be set on bad call';
ok $DBI::errstr, 'error string should be set on a bad call';

$dbh->disconnect;
