use strict;
use warnings;

use Test::More;
use FindBin qw($Bin);
use vars qw($test_dsn $test_user $test_password $test_db $test_emboptions);

use DBD::MariaDB;

$| = 1; # flush stdout asap to keep in sync with stderr

$::COL_NULLABLE = 1;
$::COL_KEY = 2;

my $file = "$Bin/MariaDB.mtest";
BAIL_OUT "Cannot execute $file: $@" if -e $file and not eval { require $file };

$::test_dsn      = $::test_dsn      || $ENV{'DBI_DSN'}   || 'DBI:MariaDB:database=test';
$::test_user     = $::test_user     || $ENV{'DBI_USER'}  || '';
$::test_password = $::test_password || $ENV{'DBI_PASS'}  || '';

BAIL_OUT "DBI test_dsn is not valid" unless $::test_dsn =~ /^[Dd][Bb][Ii]:MariaDB:/;

if (not $::test_db) {
    my $driver_dsn = $::test_dsn;
    $driver_dsn =~ s/^[^:]*:[^:]*://;
    $::test_db = DBD::MariaDB->parse_dsn($driver_dsn)->{database};
    $::test_db = 'test' unless $::test_db;
}

sub DbiTestConnect {
    my $err;
    my $dbh = eval { DBI->connect(@_) };
    if ( $dbh ) {
        my ($current_charset, $current_collation) = $dbh->selectrow_array('SELECT @@character_set_database, @@collation_database');
        my $expected_charset = $dbh->selectrow_array("SHOW CHARSET LIKE 'utf8mb4'") ? 'utf8mb4' : 'utf8';
        my $expected_collation = "${expected_charset}_unicode_ci";
        if ($current_charset ne $expected_charset) {
            $err = "Database charset is not $expected_charset, but $current_charset";
        } elsif ($current_collation ne $expected_collation) {
            $err = "Database collation is not $expected_collation, but $current_collation";
        }
    } else {
        if ( $@ ) {
            $err = $@;
            $err =~ s/ at \S+ line \d+\.?\s*$//;
        }
        if ( not $err ) {
            $err = $DBI::errstr;
            $err = "unknown error" unless $err;
            my $user = $_[1];
            my $dsn = $_[0];
            $dsn =~ s/^DBI:[^:]+://;
            $err = "DBI connect('$dsn','$user',...) failed: $err";
        }
        my ($func, $file, $line) = caller;
        $err .= " at $file line $line.";
    }
    if ( defined $err ) {
        if ( $ENV{CONNECTION_TESTING} ) {
            BAIL_OUT "no database connection: $err";
        } else {
            plan skip_all => "no database connection: $err";
        }
    }
    return $dbh;
}

sub connection_id {
    my $dbh = shift;
    return 0 unless $dbh;

    # Paul DuBois says the following is more reliable than
    # $dbh->{'mariadb_thread_id'};
    my @row = $dbh->selectrow_array("SELECT CONNECTION_ID()");

    return $row[0];
}

# nice function I saw in DBD::Pg test code
sub byte_string {
    my $ret = join( "|" ,unpack( "C*" ,$_[0] ) );
    return $ret;
}

sub shutdown_mariadb_socket {
    my $dbh = shift;
    # shutdown needs Perl file handle but mariadb_sockfd() returns C file descriptor number
    # mode "+<&" assign a dup() copy of C file descriptor to Perl file handle and
    # mode "+<&=" assign directly C file descriptor to Perl file handle
    # close automatically close C file descriptor in Perl file handle
    # mysql client library does not expect if somebody closes its file descriptors
    # so always take a copy of mariadb_sockfd() C file descriptor and just shutdown it
    my $fd = $dbh->mariadb_sockfd();
    return undef unless defined $fd;
    open my $socket, '+<&', $fd;
    return undef unless defined $socket;
    my $ret = shutdown($socket, 2);
    close $socket;
    return $ret;
}

=item CheckRoutinePerms()

Check if the current user of the DBH has permissions to create/drop procedures

    if (!CheckRoutinePerms($dbh)) {
        plan skip_all =>
            $dbh->errstr();
    }

=cut

sub CheckRoutinePerms {
    my $dbh = shift @_;

    # check for necessary privs
    local $dbh->{PrintError} = 0;
    if (not eval { $dbh->do('DROP PROCEDURE IF EXISTS testproc') }) {
        return 0 if $dbh->errstr() =~ /alter routine command denied to user/;
        return 0 if $dbh->errstr() =~ /Table 'mysql\.proc' doesn't exist/;
    }

    return 1;
};

1;
