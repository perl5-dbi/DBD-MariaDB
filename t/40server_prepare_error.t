use strict;
use warnings;

use DBI;
use Test::More;
use lib '.', 't';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

$test_dsn.= ";mariadb_server_prepare=1;mariadb_server_prepare_disable_fallback=1";
my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });

plan tests => 3;

# execute invalid SQL to make sure we get an error
my $q = "select select select";	# invalid SQL
my $sth;
eval {$sth = $dbh->prepare($q);};
ok $dbh->err;
ok $dbh->errstr and note 'errstr: ' . $dbh->errstr;

ok $dbh->disconnect();
