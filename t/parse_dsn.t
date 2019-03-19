use strict;
use warnings;

use Test::Deep;
use Test::More;
use DBD::MariaDB;

plan tests => 8;

my @dsn_localhost_test = qw(
    db=test;hostname=localhost;port=3306
    dbname=test;host=localhost;port=3306
    database=test;host=localhost:3306
    test:localhost:3306
    test:localhost:3306;notarealdb:notarealhost:1234
    notarealdb:notarealhost:1234;database=test;host=localhost;port=3306
    database=test;host=localhost;port=3306;notarealdb:notarealhost:1234
);
my %attrs_localhost_test = (
    database => 'test',
    host     => 'localhost',
    port     => 3306,
);
foreach my $dsn (@dsn_localhost_test) {
    my $attrs = DBD::MariaDB->parse_dsn($dsn);
    cmp_deeply($attrs, \%attrs_localhost_test, "$dsn parsed correctly");
}

{
    my %attrs_ipv6_test = (
        database => 'test',
        host     => '::1',
        port     => 3306,
    );
    my $dsn   = 'database=test;host=[::1]:3306';
    my $attrs = DBD::MariaDB->parse_dsn($dsn);
    cmp_deeply($attrs, \%attrs_ipv6_test, "$dsn parsed correctly");
}

