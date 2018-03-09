#!perl -w
# vim: ft=perl
#
#   This checks for UTF-8 parameter support.
#

use strict;
use warnings FATAL => 'all';
use DBI;
use DBI::Const::GetInfoType;
use Carp qw(croak);
use Test::More;
use vars qw($table $test_dsn $test_user $test_password); 
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1 });

my $nasty_unicode1 = "\N{U+C3}\N{U+BF}"; # looks like character 0xff, if you accidentally utf8 decode
utf8::downgrade($nasty_unicode1);
my $nasty_unicode2 = $nasty_unicode1;
utf8::upgrade($nasty_unicode2);

is($nasty_unicode1, $nasty_unicode2, "Perl's internal form does not matter");

my $nasty_bytes1 = chr(0xc3).chr(0xbf); # looks like character 0xff, if you accidentally utf8 decode
utf8::downgrade($nasty_bytes1);
my $nasty_bytes2 = $nasty_bytes1;
utf8::upgrade($nasty_bytes2);

is($nasty_bytes1, $nasty_bytes2, "Perl's internal form does not matter");

is($nasty_unicode1, $nasty_bytes1, "Perl does not distinguish between bytes and Unicode string");
is($nasty_unicode2, $nasty_bytes2, "Perl does not distinguish between bytes and Unicode string");

foreach my $server_prepare (0, 1) {

    my $enable_str = "mariadb_server_prepare=$server_prepare";
    my $enable_hash = { mariadb_server_prepare => $server_prepare, mariadb_server_prepare_disable_fallback => 1 };

    $dbh = DBI->connect($test_dsn, $test_user, $test_password, $enable_hash) or die DBI->errstr;

    foreach my $charset ("latin1", "utf8") {

        $dbh->do("DROP TABLE IF EXISTS unicode_test");
        $dbh->do(qq{
            CREATE TEMPORARY TABLE unicode_test (
                payload VARCHAR(20),
                id int(10)
            ) CHARACTER SET $charset
        }) or die $dbh->errstr;


        my $nasty_unicode1_param = $nasty_unicode1;
        my $nasty_unicode2_param = $nasty_unicode2;


        $dbh->do("INSERT INTO unicode_test (id, payload) VALUES (1, ?), (2, ?)", {}, $nasty_unicode1_param, $nasty_unicode2_param);


        $dbh->do("INSERT INTO unicode_test (id, payload) VALUES (3, '$nasty_unicode1_param')");
        $dbh->do("INSERT INTO unicode_test (id, payload) VALUES (4, '$nasty_unicode2_param')");

        my $sth = $dbh->prepare("INSERT INTO unicode_test (id, payload) VALUES (?, ?)");
        $sth->execute(5, $nasty_unicode1_param);
        $sth->execute(6, $nasty_unicode2_param);

        $sth = $dbh->prepare("INSERT INTO unicode_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 7);
        $sth->bind_param(2, $nasty_unicode1_param);
        $sth->execute;

        $sth = $dbh->prepare("INSERT INTO unicode_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 8);
        $sth->bind_param(2, $nasty_unicode2_param);
        $sth->execute;

        {
            my $sql = "INSERT INTO unicode_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        $sth->execute(9, $nasty_unicode1_param);
        $sth->execute(10, $nasty_unicode2_param);

        {
            my $sql = "INSERT INTO unicode_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 11;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_unicode1_param;
            $sth->bind_param($param, $val);
        }
        $sth->execute;

        {
            my $sql = "INSERT INTO unicode_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 12;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_unicode2_param;
            $sth->bind_param($param, $val);
        }
        $sth->execute;

        my @trials = (
            'do with supplied params',
            'do with interpolated string',
            'prepare then execute',
            'prepare, bind, execute',
            'prepare (free param) then execute',
            'prepare (free param), bind (free param), execute',
        );

        for (my $i = 0; $i<@trials; $i++) {
            my $id1 = $i*2+1;
            my $id2 = $i*2+2;

            (my $out) = $dbh->selectrow_array("SELECT payload FROM unicode_test WHERE id = $id1");
            is($out, "\N{U+C3}\N{U+BF}", "unicode / $trials[$i] / utf8::downgrade / $charset / $enable_str");

            ($out) = $dbh->selectrow_array("SELECT payload FROM unicode_test WHERE id = $id2");
            is($out, "\N{U+C3}\N{U+BF}", "unicode / $trials[$i] / utf8::upgrade / $charset / $enable_str");
        }




        $dbh->do("DROP TABLE IF EXISTS blob_test");
        $dbh->do(qq{
            CREATE TEMPORARY TABLE blob_test (
                payload BLOB,
                id int(10)
            ) CHARACTER SET $charset
        }) or die $dbh->errstr;

        my $nasty_bytes1_param = $nasty_bytes1;
        my $nasty_bytes2_param = $nasty_bytes2;

        $sth = $dbh->prepare("INSERT INTO blob_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 1);
        $sth->bind_param(2, $nasty_bytes1_param, DBI::SQL_BLOB);
        $sth->execute;

        $sth = $dbh->prepare("INSERT INTO blob_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 2);
        $sth->bind_param(2, $nasty_bytes2_param, DBI::SQL_BLOB);
        $sth->execute;

        {
            my $sql = "INSERT INTO blob_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 3;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_bytes1_param;
            my $type = DBI::SQL_BLOB;
            $sth->bind_param($param, $val, $type);
        }
        $sth->execute;

        {
            my $sql = "INSERT INTO blob_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 4;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_bytes2_param;
            my $type = DBI::SQL_BLOB;
            $sth->bind_param($param, $val, $type);
        }
        $sth->execute;

        @trials = (
            'prepare, bind, execute',
            'prepare (free param), bind (free param), execute',
        );

        for (my $i = 0; $i<@trials; $i++) {
            my $id1 = $i*2+1;
            my $id2 = $i*2+2;

            (my $out) = $dbh->selectrow_array("SELECT payload FROM blob_test WHERE id = $id1");
            is($out, chr(0xc3).chr(0xbf), "blob / $trials[$i] / utf8::downgrade / $charset / $enable_str");

            ($out) = $dbh->selectrow_array("SELECT payload FROM blob_test WHERE id = $id2");
            is($out, chr(0xc3).chr(0xbf), "blob / $trials[$i] / utf8::upgrade / $charset / $enable_str");
        }

    }

}

done_testing();
