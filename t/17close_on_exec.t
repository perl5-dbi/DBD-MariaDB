use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan skip_all => "Connection to Embedded server does not have socket" if $dbh->{mariadb_hostinfo} eq 'Embedded';

plan tests => 2;

# Spawn new perl child process with MariaDB file descriptor passed as first argument and check that it is invalid (EBADF) in spawned process
# WARNING: On Windows all double quotes and newlines from command line are removed, so do not use double quotes or comments in the script below
ok(system({$^X} ($^X, '-e', << 'EOD', $dbh->mariadb_sockfd)) == 0, 'mariadb_sockfd is not inherited to spawned child process');
my $fd = $ARGV[0];
open my $socket, '+<&=', $fd and die '# Error: File descriptor is available in spawned child process'.qq(\n);
$!{EBADF} or die '# Error: Cannot check for file descriptor state in spawned child process: $!'.qq(\n);
exit 0;
EOD

is_deeply($dbh->selectrow_arrayref('SELECT 1'), [1], 'connection with server is working after spawning child process');
