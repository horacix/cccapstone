raw_flights = load '/user/otp/flights_*' using PigStorage(',');
flights = FOREACH raw_flights GENERATE $4 as airport, $5 as destination, $7 as dep_delay;
fflights = FILTER flights BY dep_delay is not null;
grouped_flights = GROUP fflights BY (airport, destination);
avg_delay = FOREACH grouped_flights GENERATE group.airport, group.destination, AVG(fflights.dep_delay) AS delay;
--describe avg_delay;

flights_by_airport = GROUP avg_delay BY airport;
--describe flights_by_airport;

out = foreach flights_by_airport {
  sorted = order avg_delay by delay asc;
  top1 = limit sorted 10;
  generate group as airport, top1.destination as dest_rank;
};
--describe out;
DUMP out;
