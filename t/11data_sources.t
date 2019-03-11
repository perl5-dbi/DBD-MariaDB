use strict;
use warnings;

use Test::Deep;
use Test::More;
use DBI;
use DBD::MariaDB;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

my @parts = DBI->parse_dsn($test_dsn);
my $driver_dsn = DBD::MariaDB->parse_dsn($parts[4]);
my %attributes = %{$driver_dsn};
%attributes = (%attributes, %{$parts[3]}) if defined $parts[3];
$attributes{user} = $test_user;
$attributes{password} = $test_password;
delete $attributes{database};

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan tests => 5;

my @dsns1 = $dbh->data_sources();
cmp_deeply(\@dsns1, supersetof("DBI:MariaDB:$test_db"), '$dbh->data_sources() contains current used database');

my @dsns2 = DBI->data_sources('MariaDB', \%attributes);
cmp_deeply(\@dsns2, supersetof("DBI:MariaDB:$test_db"), 'DBI->data_sources(\'MariaDB\') contains current used database');

my @ds_localhost_test = qw(
    hostname=localhost;db=test
    host=localhost;dbname=test
);
my %attrs_localhost_test = (
    host     => 'localhost',
    database => 'test',
);
foreach my $dsn (@ds_localhost_test) {
    my $attrs = DBD::MariaDB->parse_dsn($dsn);
    cmp_deeply($attrs, \%attrs_localhost_test, "$dsn parsed correctly");
}

{
    my %attrs_ipv6_test = (
        host     => '::1',
        database => 'test',
    );
    my $dsn   = 'host=[::1];database=test';
    my $attrs = DBD::MariaDB->parse_dsn($dsn);
    cmp_deeply($attrs, \%attrs_ipv6_test, "$dsn parsed correctly");
}
