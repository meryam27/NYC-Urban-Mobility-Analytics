
select cast(d as timestamp(3)) as date_day,
       day_of_week(d) as dow,
       month(d) as month_num,
       year(d) as year_num,
       day_of_week(d) in (6, 7) as is_weekend
from unnest(sequence(date '2024-01-01', date '2027-12-31', interval '1' day)) as t(d)