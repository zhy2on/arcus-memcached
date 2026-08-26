#!/usr/bin/perl
# Measures actual memory usage (SM:used_total_space) of a map collection's
# underlying hash tree while inserting a large number of randomly-keyed
# fields, to gauge the real-world cost of widening htree_node's hcnt field
# (int16_t -> int32_t) done alongside the depth-capped split behavior in
# hash_tree.c.
#
# This is a manual measurement tool, not a pass/fail correctness test:
# it always prints a report and asserts only basic sanity (collection
# was created and holds elements).
#
# Usage:
#   perl -I t/lib t/coll_mop_htree_memory_usage.t [count]
#   (default count: 1,000,000; capped at MAXIMUM_MAX_COLL_SIZE)

use strict;
use warnings;
use Test::More tests => 4;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $count = shift(@ARGV) || 1_000_000;
$count = 1_000_000 if $count > 1_000_000; # MAXIMUM_MAX_COLL_SIZE

my $server = get_memcached($engine, "-m 1024 -e max_map_size=$count");
my $sock = $server->sock;
$sock->autoflush(1);

my $key = "htree_mem_test_map";
my $batch_size = 5000;

sub recv_n_lines {
    my ($sock, $n) = @_;
    my @lines;
    for (1..$n) {
        my $line = <$sock>;
        push(@lines, $line);
    }
    return @lines;
}

# first insert creates the collection
print $sock "mop insert $key 0 1 create 0 0 $count\r\n" . "v" . "\r\n";
my ($first_resp) = recv_n_lines($sock, 1);
like($first_resp, qr/^(CREATED_STORED|STORED)/, "map created and first element stored");

# insert remaining elements with randomized field names, pipelined in
# batches so the round-trip latency of one-request-at-a-time doesn't
# dominate the measurement.
my $inserted = 1;
my $bad_responses = 0;
while ($inserted < $count) {
    my $n = ($count - $inserted < $batch_size) ? ($count - $inserted) : $batch_size;
    my $buf = "";
    for (my $j = 0; $j < $n; $j++) {
        my $field = int(rand(2**31));
        $buf .= "mop insert $key $field 1\r\nv\r\n";
    }
    print $sock $buf;

    my @lines = recv_n_lines($sock, $n);
    for my $line (@lines) {
        # STORED or ELEMENT_EXISTS (duplicate random field) are both fine;
        # anything else indicates a real problem.
        $bad_responses++ unless $line =~ /^(STORED|ELEMENT_EXISTS)/;
    }
    $inserted += $n;
}

my $stats = mem_stats($sock, "slabs");

print $sock "getattr $key count\r\n";
my $attr_resp;
while (<$sock>) {
    last if /^END/;
    $attr_resp = $_;
}
my ($actual_count) = ($attr_resp =~ /count=(\d+)/);
ok(defined $actual_count && $actual_count > 0, "collection holds elements ($actual_count)");

my $used_total_space = $stats->{'SM:used_total_space'};
ok(defined $used_total_space, "SM:used_total_space reported by stats slabs");

diag("");
diag("==== htree memory usage report ====");
diag(sprintf("requested inserts        : %d", $count));
diag(sprintf("bad responses            : %d", $bad_responses));
diag(sprintf("actual element count     : %s", defined $actual_count ? $actual_count : "?"));
diag(sprintf("SM:used_total_space (B)  : %s", defined $used_total_space ? $used_total_space : "?"));
if (defined $used_total_space && defined $actual_count && $actual_count > 0) {
    diag(sprintf("SM space per element (B) : %.4f", $used_total_space / $actual_count));
}
diag("====================================");

ok(1, "memory usage report generated");

release_memcached($engine, $server);
