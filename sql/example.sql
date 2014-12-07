# This is a SQL script that demonstrates the various features of the hyperloglog functions

# create raw data table
drop table if exists user_visits;
create table user_visits (
  id auto_increment,
  url varchar(255),
  user_id integer,
  visit_time datetime,
  visit_length_in_minutes integer
);

copy user_visits(url, user_id, visit_time FORMAT 'YYYY-MM-DD HH:MI:SS', visit_length_in_minutes) from '/vagrant/vertica-hyperloglog/sql/data.csv' DELIMITER ',';

# get accurate counts for google visits per day
select date(visit_time), count(distinct user_id) accurate_user_count
from user_visits
where url like '%google%'
group by date(visit_time)
order by 1;

# get estimated counts for google visits per day
select date(visit_time), hll_compute(cast(user_id as varchar)) estimated_user_count
from user_visits
where url like '%google%'
group by date(visit_time)
order by 1;

# get accurate counts for the last 3 days per url
select url, count(distinct user_id) accurate_user_count
from user_visits
where visit_time >= '2014-06-13'
group by url
order by 1;

# get estimated counts for the last 3 days per url
select url, hll_compute(cast(user_id as varchar)) estimated_user_count
from user_visits
where visit_time >= '2014-06-13'
group by url
order by 1;

# create aggregated table
drop table if exists daily_user_visits;
create table daily_user_visits (
  day date,
  url varchar(255),
  user_hll varchar(6000),
  visit_length_in_minutes integer,
  unique(day, url)
);

insert into daily_user_visits(day, url, user_hll, visit_length_in_minutes)
select date(visit_time), url, hll_create(cast(user_id as varchar)), sum(visit_length_in_minutes)
from user_visits
group by date(visit_time), url;


# get estimated counts for google visits per day
select day, hll_merge_compute(user_hll) estimated_user_count
from daily_user_visits
where url like '%google%'
group by day
order by 1;

# get accurate and estimated counts for the last 3 days per url
select url, hll_merge_compute(user_hll) estimated_user_count
from daily_user_visits
where day >= '2014-06-13'
group by url
order by 1;


# build aggregation incrementally

truncate table daily_user_visits;

# first insert, no unique violation
insert into daily_user_visits(day, url, user_hll, visit_length_in_minutes)
select date(visit_time), url, hll_create(cast(user_id as varchar)), sum(visit_length_in_minutes)
from user_visits
where id < 10000
group by date(visit_time), url;

drop table if exists daily_user_visits_temp;
create table daily_user_visits_temp (
  day date,
  url varchar(255),
  user_hll varchar(6000),
  visit_length_in_minutes integer,
  unique(day, url)
);

insert into daily_user_visits_temp(day, url, user_hll, visit_length_in_minutes)
select date(visit_time), url, hll_create(cast(user_id as varchar)), sum(visit_length_in_minutes)
from user_visits
where id >= 10000
group by date(visit_time), url;

merge into daily_user_visits d using daily_user_visits_temp v on (d.url = v.url and d.day = v.day)
when matched then update set visit_length_in_minutes = d.visit_length_in_minutes + v.visit_length_in_minutes, user_hll = HLL_MERGE2(d.user_hll, v.user_hll);

drop table if exists daily_user_visits_temp;

