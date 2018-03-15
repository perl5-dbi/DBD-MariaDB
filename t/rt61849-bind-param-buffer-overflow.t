use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $INSECURE_VALUE_FROM_USER = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, AutoCommit => 0 });

plan tests => 3;
my $sth = $dbh->prepare("select * from unknown_table where id=?");
ok !defined eval { $sth->bind_param(1, $INSECURE_VALUE_FROM_USER, 3) };
like $sth->errstr(), qr/Binding non-numeric field 1, value '\Q$INSECURE_VALUE_FROM_USER\E' as a numeric!/, "bind_param failed on incorrect numeric value";
pass "perl interpreter did not crash";
