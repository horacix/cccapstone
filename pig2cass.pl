#!/usr/bin/perl -w

while (<>) {
  if ($_ =~ /([A-Z]{3}),/g) {
    $airport = $1;
    @carriers = [];
    while ($_ =~ /([A-Z0-9]{2})/g) {
      push(@carriers, $1);
    }
    shift @carriers;
    $list = join("','", @carriers);
    print "INSERT INTO coursera.carrier_rank_by_airport (airport, carrier_rank) VALUES ('$airport', ['$list']);\n"
  }
}
