use strict;
use warnings;

use vars qw($test_dsn $test_user $test_password);
use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

if ($dbh->{mariadb_serverversion} < 50008) {
  plan skip_all => "Servers < 5.0.8 do not support b'' syntax";
}

if ($dbh->{mariadb_serverversion} < 50026) {
  plan skip_all => "Servers < 5.0.26 do not support BIN() for BIT values";
}

if ($dbh->{mariadb_clientversion} < 50003) {
  plan skip_all => "Clients < 5.0.3 do not support BIT type";
}

$dbh->disconnect();

for my $scenario (qw(prepare noprepare)) {

my $sth;

my $dsn = $test_dsn;
$dsn .= ';mariadb_server_prepare=1;mariadb_server_prepare_disable_fallback=1' if $scenario eq 'prepare';
$dbh = DBI->connect($dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });

my $create = <<EOT;
CREATE TEMPORARY TABLE `dbd_mysql_rt88006_bit_prep` (
  `id` bigint(20) NOT NULL auto_increment,
  `flags` bit(40) NOT NULL,
  PRIMARY KEY  (`id`),
  KEY `flags` (`flags`)
)
EOT

ok $dbh->do($create),"create table for $scenario";

ok $dbh->do("INSERT INTO dbd_mysql_rt88006_bit_prep (id, flags) VALUES (1, b'10'), (2, b'1'), (3, b'1111011111101111101101111111101111111101')");

ok $sth = $dbh->prepare("INSERT INTO dbd_mysql_rt88006_bit_prep (id, flags) VALUES (?, ?)");
ok $sth->bind_param(1, 4, DBI::SQL_INTEGER);
ok $sth->bind_param(2, pack("B*", '1110000000000000011101100000000011111101'), DBI::SQL_BINARY);
ok $sth->execute();

ok $sth = $dbh->prepare("SELECT id,flags FROM dbd_mysql_rt88006_bit_prep WHERE id = 1");
ok $sth->execute();
ok (my $r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario");
is ($r->{id}, 1, 'id test contents');
is (unpack("B*", $r->{flags}), '0000000000000000000000000000000000000010', 'flags has contents');

ok $sth = $dbh->prepare("SELECT id,flags FROM dbd_mysql_rt88006_bit_prep WHERE id = 3");
ok $sth->execute();
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with more then 32 bits");
is ($r->{id}, 3, 'id test contents');
is (unpack("B*", $r->{flags}), '1111011111101111101101111111101111111101', 'flags has contents');

ok $sth = $dbh->prepare("SELECT id,flags FROM dbd_mysql_rt88006_bit_prep WHERE id = 4");
ok $sth->execute();
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with binary insert");
is ($r->{id}, 4, 'id test contents');
is (unpack("B*", $r->{flags}), '1110000000000000011101100000000011111101', 'flags has contents');

ok $sth = $dbh->prepare("SELECT id,BIN(flags) FROM dbd_mysql_rt88006_bit_prep WHERE ID =1");
ok $sth->execute();
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with BIN()");
is ($r->{id}, 1, 'id test contents');
is ($r->{'BIN(flags)'}, '10', 'flags has contents');

ok $sth = $dbh->prepare("SELECT id,BIN(flags) FROM dbd_mysql_rt88006_bit_prep WHERE ID =3");
ok $sth->execute();
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with BIN() and more then 32 bits");
is ($r->{id}, 3, 'id test contents');
is ($r->{'BIN(flags)'}, '1111011111101111101101111111101111111101', 'flags has contents');

ok $sth = $dbh->prepare("SELECT id,BIN(flags) FROM dbd_mysql_rt88006_bit_prep WHERE ID =4");
ok $sth->execute();
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with BIN() and with binary insert");
is ($r->{id}, 4, 'id test contents');
is ($r->{'BIN(flags)'}, '1110000000000000011101100000000011111101', 'flags has contents');

ok $dbh->disconnect;
}

done_testing;
