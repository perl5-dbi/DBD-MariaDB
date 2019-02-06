use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

use constant SHOW_PROGRESS => ($ENV{SHOW_PROGRESS} ? 1 : 0);

my $COUNT_CONNECT = 4000;     # Number of connect/disconnect iterations
my $COUNT_PREPARE = 30000;    # Number of prepare/execute/finish iterations
my $COUNT_BIND    = 10000;    # Number of bind_param iterations

my $have_storable;

BEGIN {
    if (!$ENV{EXTENDED_TESTING}) {
        plan skip_all => "Skip \$ENV{EXTENDED_TESTING} is not set\n";
    }
    if (not eval { require Proc::ProcessTable }) {
        plan skip_all => "module Proc::ProcessTable not installed \n";
    }
    $have_storable = eval { require Storable } ? 1 : 0;
}

my $have_pt_size = grep { $_ eq 'size' } Proc::ProcessTable->new('cache_ttys' => $have_storable)->fields;
if (!$have_pt_size) {
        plan skip_all => "module Proc::ProcessTable does not support size attribute on current platform\n";
}

plan skip_all => 'this test is not supported on OpenBSD platform' if $^O eq 'openbsd';

my ($dbh, $sth);
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                                            { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
$dbh->disconnect;
plan tests => 30 * 2;

if (not SHOW_PROGRESS and $ENV{TEST_VERBOSE}) {
    note "You can set \$ENV{SHOW_PROGRESS} to monitor the progress of slow tests";
}

sub size {
    my($p, $pt);
    $pt = Proc::ProcessTable->new('cache_ttys' => $have_storable);
    for $p (@{$pt->table()}) {
        if ($p->pid() == $$) {
            return $p->size();
        }
    }
    die "Cannot find my own process?!?\n";
    exit 0;
}

for my $mariadb_server_prepare (0, 1) {

note "Testing memory leaks with mariadb_server_prepare=$mariadb_server_prepare\n";

$dbh = DBI->connect($test_dsn, $test_user, $test_password,
                   { RaiseError => 1, PrintError => 0, AutoCommit => 0, mariadb_server_prepare => $mariadb_server_prepare, mariadb_server_prepare_disable_fallback => 1 });

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t60leaks");

my $create= <<EOT;
CREATE TABLE dbd_mysql_t60leaks (
    id INT(4) NOT NULL DEFAULT 0,
    name VARCHAR(64) NOT NULL DEFAULT ''
    )
EOT

ok $dbh->do($create);

my ($size, $prev_size, $ok, $not_ok, $dbh2, $msg);
note "Testing memory leaks in connect/disconnect\n";
$msg = "Possible memory leak in connect/disconnect detected";

$ok = 0;
$not_ok = 0;
$prev_size= undef;

for (my $i = 0;    $i < $COUNT_CONNECT;    $i++) {
    $dbh2 = DBI->connect($test_dsn, $test_user, $test_password,
                               { RaiseError => 1, 
                                 PrintError => 0,
                                 AutoCommit => 0,
                                 mariadb_server_prepare => $mariadb_server_prepare,
                               });

    if ($i % 100    ==    99) {
        $size = size();
        if (defined($prev_size)) {
            if ($size == $prev_size) {
                $ok++;
            }
            else {
                $not_ok++;
            }
        }
        else {
            $prev_size = $size;
            $size = size();
        }
        $prev_size = $size;
    }
    if (SHOW_PROGRESS) {
        note sprintf "Progress: %5d/%5d", $i+1, $COUNT_CONNECT;
    }
}
$dbh2->disconnect;

ok $ok, "\$ok $ok";
ok !$not_ok, "\$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in auto_reconnect\n";
$msg = "Possible memory leak in auto_reconnect";

$ok = 0;
$not_ok = 0;
undef $prev_size;

$dbh2 = DBI->connect($test_dsn, $test_user, $test_password,
                     { RaiseError => 1,
                       PrintError => 0,
                       mariadb_server_prepare => $mariadb_server_prepare,
                       mariadb_auto_reconnect => 1,
                     });
for (my $i = 0; $i < $COUNT_CONNECT; $i++) {
    $dbh2->disconnect;
    $dbh2->do("SELECT 1");
    if ($i % 100 == 99) {
        $size = size();
        if (defined($prev_size)) {
            if ($size == $prev_size) {
                $ok++;
            }
            else {
                $not_ok++;
            }
        }
        else {
            $prev_size = $size;
            $size = size();
        }
        $prev_size = $size;
    }
    if (SHOW_PROGRESS) {
        note sprintf "Progress: %5d/%5d", $i+1, $COUNT_CONNECT;
    }
}
$dbh2->disconnect;

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in prepare/execute/finish\n";
$msg = "Possible memory leak in prepare/execute/finish detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0; $i < $COUNT_PREPARE; $i++) {
    my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t60leaks");
    $sth->execute();
    $sth->finish();

    if ($i % 100 == 99) {
        $size = size();
        if (defined($prev_size))
        {
            if ($size == $prev_size) {
                $ok++;
            }
            else {
                $not_ok++;
            }
        }
        else {
            $prev_size = $size;
            $size = size();
        }
        $prev_size = $size;
    }
    if (SHOW_PROGRESS and $i % 10 == 9) {
        note sprintf "Progress: %5d/%5d", $i+1, $COUNT_PREPARE;
    }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in execute/finish\n";
$msg = "Possible memory leak in execute/finish detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

{
    my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t60leaks");

    for (my $i = 0; $i < $COUNT_PREPARE; $i++) {
        $sth->execute();
        $sth->finish();

        if ($i % 100 == 99) {
            $size = size();
            if (defined($prev_size))
            {
                if ($size == $prev_size) {
                    $ok++;
                }
                else {
                    $not_ok++;
                }
            }
            else {
                $prev_size = $size;
                $size = size();
            }
            $prev_size = $size;
        }
        if (SHOW_PROGRESS and $i % 10 == 9) {
            note sprintf "Progress: %5d/%5d", $i+1, $COUNT_PREPARE;
        }
    }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in bind_param\n";
$msg = "Possible memory leak in bind_param detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

{
    my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t60leaks WHERE id = ? AND name = ?");

    for (my $i = 0; $i < $COUNT_BIND; $i++) {
        $sth->bind_param(1, 0);
        my $val = "x" x 1000000;
        $sth->bind_param(2, $val);

        if ($i % 100 == 99) {
            $size = size();
            if (defined($prev_size))
            {
                if ($size == $prev_size) {
                    $ok++;
                }
                else {
                    $not_ok++;
                }
            }
            else {
                $prev_size = $size;
                $size = size();
            }
            $prev_size = $size;
        }
        if (SHOW_PROGRESS and $i % 10 == 9) {
            note sprintf "Progress: %5d/%5d", $i+1, $COUNT_BIND;
        }
    }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in fetchrow_arrayref\n";
$msg= "Possible memory leak in fetchrow_arrayref detected";

$sth= $dbh->prepare("INSERT INTO dbd_mysql_t60leaks VALUES (?, ?)") ;

my $dataref= [[1, 'Jochen Wiedmann'],
    [2, 'Andreas Konig'],
    [3, 'Tim Bunce'],
    [4, 'Alligator Descartes'],
    [5, 'Jonathan Leffler']];

for (@$dataref) {
    ok $sth->execute($_->[0], $_->[1]),
        "insert into dbd_mysql_t60leaks values ($_->[0], '$_->[1]')";
}

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0; $i < $COUNT_PREPARE; $i++) {
    {
        my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t60leaks");
        $sth->execute();
        my $row;
        while ($row = $sth->fetchrow_arrayref()) { }
    }

    if ($i % 100 == 99) {
        $size = size();
        if (defined($prev_size)) {
            if ($size == $prev_size) {
                ++$ok;
            }
            else {
                ++$not_ok;
            }
        }
        else {
                $prev_size = $size;
                $size = size();
        }
        $prev_size = $size;
    }
    if (SHOW_PROGRESS and $i % 10 == 9) {
        note sprintf "Progress: %5d/%5d", $i+1, $COUNT_PREPARE;
    }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

note "Testing memory leaks in fetchrow_hashref\n";
$msg = "Possible memory leak in fetchrow_hashref detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0; $i < $COUNT_PREPARE; $i++) {
    {
        my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t60leaks");
        $sth->execute();
        my $row;
        while ($row = $sth->fetchrow_hashref()) { }
    }

    if ($i % 100 == 99) {
        $size = size();
        if (defined($prev_size)) {
            if ($size == $prev_size) {
                ++$ok;
            }
            else {
                ++$not_ok;
            }
        }
        else {
            $prev_size = $size;
            $size = size();
        }
        $prev_size = $size;
    }
    if (SHOW_PROGRESS and $i % 10 == 9) {
        note sprintf "Progress: %5d/%5d", $i+1, $COUNT_PREPARE;
    }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

ok $dbh->do("DROP TABLE dbd_mysql_t60leaks");
ok $dbh->disconnect;

}
