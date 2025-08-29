use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

plan tests => 1;

my $dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 0, mariadb_ssl => 0, mariadb_ssl_verify_server_cert => 1 });
ok(! defined $dbh && $DBI::errstr =~ /but SSL encryption was not enabled/,
   'DBD::MariaDB cannot use other ssl options if ssl is not enabled'
) or diag(defined $dbh ? 'Connection succeeded' : "Unexpected error: $DBI::errstr");
