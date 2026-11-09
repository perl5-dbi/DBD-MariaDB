use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

sub get_status {
  my ($fd) = @_;
  my $bits = '';
  vec($bits, $fd, 1) = 1;
  my $nfound = select(my $rout = $bits, my $wout = $bits, my $eout = $bits, 0);
  return ($nfound, $rout, $wout, $eout);
}

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan skip_all => "Connection to Embedded server does not have socket" if $dbh->{mariadb_hostinfo} eq 'Embedded';

plan tests => 11;

my $orig_fd = $dbh->mariadb_sockfd();
cmp_ok($orig_fd, '>=', 0);
{
  # test that orig fd is in the ready state for writing and there is no pending data or error condition
  my ($nfound, $rout, $wout, $eout) = get_status($orig_fd);
  is($nfound, 1);
  ok(!vec($rout, $orig_fd, 1));
  ok(vec($wout, $orig_fd, 1));
  ok(!vec($eout, $orig_fd, 1));
}

# duplicate file descriptor $orig_fd, assign it to the perl handle socket and get duplicated file descriptor
open my $socket, '+<&', $orig_fd;
my $cloned_fd = fileno($socket);
cmp_ok($cloned_fd, '>=', 0);

# Set InactiveDestroy and destroy $dbh, it should not disconnect/shutdown connection to the server
$dbh->{InactiveDestroy} = 1;
undef $dbh;

{
  # test that orig fd is not valid anymore, select failed
  my ($nfound) = get_status($orig_fd);
  cmp_ok($nfound, '<', 0);
}

{
  # test that cloned fd is in the ready state for writing and there is no pending data or error condition
  # in case the connection was disconnected, there would be pending zero-length mark which indicates that other side closed socket
  my ($nfound, $rout, $wout, $eout) = get_status($cloned_fd);
  is($nfound, 1);
  ok(!vec($rout, $cloned_fd, 1));
  ok(vec($wout, $cloned_fd, 1));
  ok(!vec($eout, $cloned_fd, 1));
}

close $socket;
