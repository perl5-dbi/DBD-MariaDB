use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh1 = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan tests => 19;

my $err1;
my $err2;
# set HandleError with catching of all errors
$dbh1->{HandleError} = sub { $err1 = $_[0]; };
{
# localize HandleError with different closure and ensure that the original one is restored after the disconnect()
local $dbh1->{HandleError} = sub { $err2 = $_[0]; };
ok $dbh1->{Active};
ok $dbh1->disconnect();
ok !$dbh1->{Active};
}
ok !defined $err1 or diag $err1;
ok !defined $err2 or diag $err2;

ok my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });
ok my $dbh3 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

ok $dbh2->{Active};
ok $dbh2->disconnect();
ok !$dbh2->{Active};

ok $dbh3->{Active};
ok $dbh3->disconnect();
ok !$dbh3->{Active};

ok my $dbh4 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });
ok our $dbh5 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

ok $dbh4->{Active};
ok $dbh5->{Active};

# install a handler so that a warning about unfreed resources gets caught
$SIG{__WARN__} = sub { die @_ };

DBI->disconnect_all();

ok !$dbh4->{Active};
ok !$dbh5->{Active};
