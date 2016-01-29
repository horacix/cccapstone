select leg1.dep_date, leg1.origin, leg1.dest, leg2.dest, leg1.airline, leg1.number, leg2.airline, leg2.number
from (
  select a.dep_date, a.origin, a.dest, a.airline, a.number, a.arr_delay 
  from flights a join (select dep_date, origin, dest, min(arr_delay) as marr_delay
					   from flights
					   where year(dep_date)=2008 and dep_time < 1200
					   group by dep_date, origin, dest) b on
  a.dep_date = b.dep_date and a.origin=b.origin and a.dest=b.dest and a.arr_delay=b.marr_delay
  where year(a.dep_date)=2008 and dep_time < 1200 ) leg1
join (
  select a.dep_date, a.origin, a.dest, a.airline, a.number, a.arr_delay 
  from flights a join (select dep_date, origin, dest, min(arr_delay) as marr_delay
					   from flights
					   where year(dep_date)=2008 and dep_time > 1200
					   group by dep_date, origin, dest) b on
  a.dep_date = b.dep_date and a.origin=b.origin and a.dest=b.dest and a.arr_delay=b.marr_delay
  where year(a.dep_date)=2008 and dep_time > 1200 ) leg2
on leg1.dest=leg2.origin AND leg1.dep_date=date_sub(leg2.dep_date,2);
