use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh1 = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1 });

plan tests => 17;

ok $dbh1->{Active};
ok $dbh1->disconnect();
ok !$dbh1->{Active};

ok my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1 });
ok my $dbh3 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1 });

ok $dbh2->{Active};
ok $dbh2->disconnect();
ok !$dbh2->{Active};

ok $dbh3->{Active};
ok $dbh3->disconnect();
ok !$dbh3->{Active};

ok my $dbh4 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1 });
ok our $dbh5 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1 });

ok $dbh4->{Active};
ok $dbh5->{Active};

# install a handler so that a warning about unfreed resources gets caught
$SIG{__WARN__} = sub { die @_ };

DBI->disconnect_all();

ok !$dbh4->{Active};
ok !$dbh5->{Active};
