set hive.exec.mode.local.auto=true;

-- 1
select live_id, max(num)
from (select live_id,
             sum(flag) over (partition by live_id order by `timestamp` ) num
      from (select live_id, in_datetime `timestamp`, 1 flag
            from live_events
            union all
            select live_id, out_datetime, -1
            from live_events) t1) t2
group by live_id;

-- 2
select user_id, concat(user_id, " - ", sum(sub) over (partition by user_id order by view_timestamp))
from (select user_id,
             view_timestamp,
             if(view_timestamp - lag(view_timestamp) over (partition by user_id order by view_timestamp ) <= 60, 0,
                1) sub
      from page_view_events) t1

--3
select user_id, max(days)
from (select user_id, datediff(max(l_date), min(l_date)) + 1 as days
      from (select user_id,l_date,
                   sum(flag)
                       over (partition by user_id order by l_date rows between unbounded preceding and current row ) flag2
            from (select user_id,
                         l_date,
                         if(datediff(l_date, lag(l_date) over (partition by user_id order by l_date)) <= 2, 0, 1) flag
                  from (select distinct user_id, date(login_datetime) l_date
                        from login_events) t1) t2) t3
      group by user_id, flag2) t4
group by user_id;

-- 4
-- 方式一
select brand, sum(total)
from (select brand, datediff(max(new_e_date), new_s_date) + 1 total
      from (select brand,
                   if(s_date <
                      max(e_date)
                          over (partition by brand order by s_date rows between unbounded preceding and 1 preceding),
                      min(s_date) over (partition by brand order by s_date), s_date) new_s_date,
                   max(e_date) over (partition by brand order by s_date)             new_e_date
            from (select brand, date(start_date) s_date, date(end_date) e_date from promotion_info) t1) t2
      group by brand, new_s_date) t3
group by brand;

-- 方式二
select distinct brand,
                sum(datediff(
                            max(date(end_date))
                                over (partition by brand order by date(start_date) rows between unbounded preceding and current row ),
                            if(date(start_date) < max(date(end_date))
                                                      over (partition by brand order by date(start_date) rows between unbounded preceding and 1 preceding),
                               date_add(max(date(end_date))
                                            over (partition by brand order by date(start_date) rows between unbounded preceding and 1 preceding),
                                        1),
                               date(start_date))) + 1) over (partition by brand) days
from promotion_info;
