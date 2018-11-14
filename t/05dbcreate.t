use strict;
use warnings;

use Test::More ;
use DBI;
use DBD::MariaDB;
$|= 1;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

# remove database from DSN
my ($dbi_dsn, $driver_dsn) = ($test_dsn =~ /^([^:]*:[^:]*:)(.*)$/);
my $attr_dsn = DBD::MariaDB->parse_dsn($driver_dsn);
delete $attr_dsn->{database};
my $test_dsn_without_db = $dbi_dsn . join ';', map { $_ . '=' . $attr_dsn->{$_} } sort keys %{$attr_dsn};

sub fatal_error {
    my ($message) = @_;
    my $err = $DBI::errstr;
    if (not $err) {
      $err = $@;
      $err =~ s/ at \S+ line \d+\.?\s*$//;
      $err = "unknown error" unless $err;
    }
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "$message: $err";
    } else {
        plan skip_all => "$message: $err";
    }
}

sub connect_to_server {
    return eval { DBI->connect($test_dsn_without_db, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 1 }) };
}

sub connect_to_database {
    return eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 1 }) };
}

my $dbh = connect_to_database();
if (not $dbh) {
    $dbh = connect_to_server();
    fatal_error "Cannot connect to '$test_dsn_without_db' server" unless $dbh;
    diag "Connected to server '$test_dsn_without_db'";

    my $failed = not eval { $dbh->do("CREATE DATABASE IF NOT EXISTS " . $dbh->quote_identifier($test_db)) };
    fatal_error "Cannot create database '$test_db' on '$test_dsn_without_db' for user '$test_user'" if $failed;
    diag "Created database '$test_db'";

    $dbh->disconnect();
    diag "Disconnected from server '$test_dsn_without_db'";

    $dbh = connect_to_database();
    fatal_error "Cannot connect to '$test_db' database on '$test_dsn' with user '$test_user'" unless $dbh;
}

diag "Connected to '$test_db' database on '$test_dsn' server";

my $charset = $dbh->selectrow_array('SELECT @@character_set_database');
diag "Database '$test_db' has charset '$charset'";

if ($charset ne 'utf8mb4') {
    my $newcharset = $dbh->selectrow_array("SHOW CHARSET LIKE 'utf8mb4'") ? 'utf8mb4' : 'utf8';
    if ($newcharset ne $charset) {
        my $failed = not eval { $dbh->do("ALTER DATABASE " . $dbh->quote_identifier($test_db) . " CHARACTER SET '$newcharset'") };
        fatal_error "No permission to change charset for '$test_db' database on '$test_dsn' for user '$test_user'" if $failed;
        diag "Changed charset for '$test_db' database to '$newcharset'";
    }
}

$dbh->disconnect();

plan tests => 1;
pass "Database '$test_db' is ready for DBD::MariaDB tests";
