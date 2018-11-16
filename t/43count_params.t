use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';
use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan tests => 37*2+1;

for my $mariadb_server_prepare (0, 1) {

$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;

ok ($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t43count_params"));

my $create = <<EOT;
CREATE TABLE dbd_mysql_t43count_params (
        id int(4) NOT NULL default 0,
        name varchar(100) default ''
        )
EOT

ok ($dbh->do($create));

ok (my $sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (name, id)" .
           " VALUES ('Charles de Batz de Castelmore, comte d\\'Artagnan', ?)"));

ok ($sth->execute(1));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (name, id)" .
                         " VALUES ('Charles de Batz de Castelmore, comte d\\'Artagnan', 2)"));

ok ($sth->execute());

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (name, id) VALUES (?, ?)"));

ok ($sth->execute("Charles de Batz de Castelmore, comte d'Artagnan", 3));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name)" .
                         " VALUES (?, 'Charles de Batz de Castelmore, comte d\\'Artagnan')"));

ok ($sth->execute(4));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name)" .
                         " VALUES (5, 'Charles de Batz de Castelmore, comte d\\'Artagnan')"));

ok ($sth->execute());

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok ($sth->execute(6, "Charles de Batz de Castelmore, comte d'Artagnan"));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (name, id)" .
                         " VALUES ('Charles \x00 de Batz de Castelmore, comte d\\'Artagnan', ?)"));

ok ($sth->execute(7));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok ($sth->execute(8, "Charles de \x00 Batz de Castelmore, comte d'Artagnan"));

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok !defined eval { $sth->execute(9) };

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok !defined eval { $sth->execute(10, "Charles de Batz de Castelmore, comte d'Artagnan", 10) };

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok !defined eval { $sth->execute() };

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok $sth->bind_param(1, 11);

ok !defined eval { $sth->execute() };

ok ($sth = $dbh->prepare("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)"));

ok $sth->bind_param(1, 12);

ok $sth->bind_param(2, "Charles de Batz de Castelmore, comte d'Artagnan");

ok !defined eval { $sth->bind_param(3, 10) };

ok !defined eval { $dbh->do("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)") };

ok !defined eval { $dbh->do("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)", undef) };

ok !defined eval { $dbh->do("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)", undef, 9) };

ok !defined eval { $dbh->do("INSERT INTO dbd_mysql_t43count_params (id, name) VALUES (?, ?)", undef, 10, "Charles de Batz de Castelmore, comte d'Artagnan", 10) };

is_deeply (
    $dbh->selectall_arrayref("SELECT id, name FROM dbd_mysql_t43count_params"),
    [
        [ 1, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 2, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 3, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 4, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 5, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 6, "Charles de Batz de Castelmore, comte d'Artagnan" ],
        [ 7, "Charles \x00 de Batz de Castelmore, comte d'Artagnan" ],
        [ 8, "Charles de \x00 Batz de Castelmore, comte d'Artagnan" ],
    ]
);

ok ($dbh->do("DROP TABLE dbd_mysql_t43count_params"));

}

ok $dbh->disconnect;
