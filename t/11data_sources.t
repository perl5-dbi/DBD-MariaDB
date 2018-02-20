use strict;
use warnings;

use Test::Deep;
use Test::More;
use DBI;
use DBD::MariaDB;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

my %attributes;
my @parts = DBI->parse_dsn($test_dsn);
%attributes = %{$parts[3]} if defined $parts[3];
DBD::MariaDB->_OdbcParse($parts[4], \%attributes, ['database', 'host', 'port']);
$attributes{user} = $test_user;
$attributes{password} = $test_password;
delete $attributes{database};

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 2;

my @dsns1 = $dbh->data_sources();
cmp_deeply(\@dsns1, supersetof("dbi:MariaDB:$test_db"), '$dbh->data_sources() contains current used database');

my @dsns2 = DBI->data_sources('MariaDB', \%attributes);
cmp_deeply(\@dsns2, supersetof("dbi:MariaDB:$test_db"), 'DBI->data_sources(\'MariaDB\') contains current used database');
