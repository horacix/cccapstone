select leg1.dep_date, leg1.origin, leg1.dest, leg2.dest, leg1.airline, leg1.number, leg2.airline, leg2.number
from (

  -- leg 1 with best arrival otp
  select f.dep_date, f.airline, f.number, f.origin, f.dest from flights f join (
  select a.airline, a.number, a.origin, a.dest from (
	select airline, number, origin, dest, avg(arr_delay) as avg_delay 
    from flights 
    where year(dep_date)=2008 and dep_time < 1200
    group by airline, number, origin, dest
  ) a join (
	select origin, dest, min(adelay) as min_avg_delay from (
	  select airline, number, origin, dest, avg(arr_delay) as adelay 
    	from flights 
    	where year(dep_date)=2008 and dep_time < 1200
    	group by airline, number, origin, dest
  	) sub1 group by origin, dest
  ) b on a.origin=b.origin and a.dest=b.dest and a.avg_delay=b.min_avg_delay
) m on f.airline = m.airline and f.number=m.number and f.origin=m.origin and f.dest=m.dest
where year(f.dep_date)=2008 and f.dep_time < 1200
  
  
) leg1 join (

  -- leg 2 with best arrival otp
select f.dep_date, f.airline, f.number, f.origin, f.dest from flights f join (
  select a.airline, a.number, a.origin, a.dest from (
	select airline, number, origin, dest, avg(arr_delay) as avg_delay 
    from flights 
    where year(dep_date)=2008 and dep_time > 1200
    group by airline, number, origin, dest
  ) a join (
	select origin, dest, min(adelay) as min_avg_delay from (
	  select airline, number, origin, dest, avg(arr_delay) as adelay 
    	from flights 
    	where year(dep_date)=2008 and dep_time > 1200
    	group by airline, number, origin, dest
  	) sub1 group by origin, dest
  ) b on a.origin=b.origin and a.dest=b.dest and a.avg_delay=b.min_avg_delay
) m on f.airline = m.airline and f.number=m.number and f.origin=m.origin and f.dest=m.dest
where year(f.dep_date)=2008 and f.dep_time > 1200
  
) leg2
on leg1.dest=leg2.origin AND leg1.dep_date=date_sub(leg2.dep_date,2);
