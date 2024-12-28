set
    hive.exec.mode.local.auto = true;

-- 2.1
select sku_id
from (select sku_id
      from (select sku_id,
                   dense_rank() over ( order by sum(sku_num) desc ) rank
            from order_detail
            group by sku_id) t1
      where rank = 2) t2
right join (select 1) t3;

-- 2.2
-- 方式一
select distinct user_id
from (select user_id,
             sum(sub) over ( partition by user_id rows between 1 preceding and current row ) days
      from (select user_id,
                   datediff(order_date, lag(order_date) over ( partition by user_id order by order_date )) sub
            from (select distinct user_id, date(create_date) order_date from order_info) t1) t2) t3
where days = 2;
-- 方式二
select distinct user_id
from (select user_id,
             count(distinct create_date)
                   over (partition by user_id order by date(create_date) range between 2 preceding and current row ) as days
      from order_info) as t1
where t1.days = 3;

-- 2.3
select *
from (select sku_info.category_id,
             category_info.category_name,
             t1.sku_id,
             sku_info.name,
             t1.order_num,
             count(*) over (
                 partition by
                     sku_info.category_id
                 ) sku_cnt,
             max(order_num) over (
                 partition by
                     sku_info.category_id
                 ) max_num_in_category
      from (select sku_id,
                   sum(sku_num) order_num
            from order_detail
            group by sku_id) t1
      join sku_info
        on t1.sku_id = sku_info.sku_id
      join category_info
        on sku_info.category_id = category_info.category_id) t2
where t2.order_num = t2.max_num_in_category;

-- 2.4
select *,
       case
           when current_all_amount < 10000 then '普通会员'
           when current_all_amount < 30000 then '青铜会员'
           when current_all_amount < 50000 then '白银会员'
           when current_all_amount < 80000 then '黄金会员'
           when current_all_amount < 100000 then '白金会员'
           else '钻石会员'
           end
from (select user_id,
             create_date,
             sum(sum_amount) over (
                 partition by
                     user_id
                 order by create_date
                 ) current_all_amount
      from (select user_id,
                   create_date,
                   sum(total_amount) sum_amount
            from order_info
            group by user_id, create_date) t1) t2;

-- 2.5
select concat(
         count(distinct user_id) / (select count(distinct user_id)
                                    from order_info) * 100, "%"
       ) rate
from (select user_id,
             datediff(
               date(create_date), date(
               first_value(create_date) over (
                   partition by
                       user_id
                   order by create_date
                   )
                                  )
             ) sub
      from order_info) t1
where sub = 1;

-- 2.9
select sku_id,
       create_date,
       num_sku
from (select *,
             dense_rank() over (
                 partition by
                     sku_id
                 order by num_sku desc
                 ) `rank`
      from (select sku_id,
                   create_date,
                   sum(sku_num) num_sku
            from order_detail
            group by sku_id, create_date) t1) t2
where `rank` = 1;

-- 2.6
select order_detail.sku_id,
       year(
         create_date),
       sum(
         sku_num),
       sum(
         sku_num * price)
from order_detail
right join (select sku_id, year(min(create_date)) first_year
            from order_detail
            group by sku_id) t1
  on order_detail.sku_id = t1.sku_id
    and year(create_date) = first_year
group by order_detail.sku_id,
         year(create_date);

-- 2.10
select *
from (select sku_id,
             sku_name,
             sum_sku_num,
             avg(sum_sku_num) over (
                 partition by
                     cate_id
                 ) avg_num,
             cate_id
      from (select order_detail.sku_id,
                   collect_set(sku_info.name)[0]        sku_name,
                   sum(sku_num)                         sum_sku_num,
                   collect_set(sku_info.category_id)[0] cate_id
            from order_detail
            join sku_info
              on order_detail.sku_id = sku_info.sku_id
            group by order_detail.sku_id) t1) t2
where sum_sku_num > avg_num;

-- 2.12
select sku_id,
       price
from (select sku_id,
             price,
             price_date,
             dense_rank() over (
                 partition by
                     sku_id
                 order by price_date desc
                 ) rank
      from (select sku_id,
                   price,
                   from_date price_date
            from sku_info
            union
            select sku_id,
                   new_price,
                   change_date
            from sku_price_modify_detail
            where date(change_date) <= date('2021-10-01')) t1) t2
where t2.rank = 1;

-- 2.13
select count(same) / count(*)
from (select user_id,
             if(
               datediff(
                 date(custom_date), date(order_date)
               ) = 0, 1, null
             )     same,
             rank() over (
                 partition by
                     user_id
                 order by order_id
                 ) rank
      from delivery_info) t1
where rank = 1;

-- 2.15
select user_id,
       min(login_date),
       max(login_date)
from (select user_id,
             login_date,
             date_sub(
               login_date, row_number() over (
                 partition by
                     user_id
                 order by login_date
                 )
             ) flag
      from (select distinct user_id,
                            date(
                              login_ts) login_date
            from user_login_detail) t1) t2
group by user_id,
         flag
having sum(1)
           > 1;

-- 2.17 答案存疑
select c_date,
       cast(
         sum(sum_amount1day) over (
             order by c_date range between 2 preceding
                 and current row
             ) as decimal(20, 2)
       ) sum_3day,
       cast(
         avg(sum_amount1day) over (
             order by c_date range between 2 preceding
                 and current row
             ) as decimal(20, 2)
       ) avg_3day
from (select date(create_date) c_date,
             sum(total_amount) sum_amount1day
      from order_info
      group by create_date) t1;

-- 2.20 答案存疑
select user_id,
       order_id,
       create_date
from (select user_id,
             order_id,
             create_date,
             rank() over (
                 partition by
                     user_id
                 order by order_id desc
                 ) rank
      from order_info
      order by user_id, order_id desc) t1
where rank <= 3
order by user_id, order_id;

-- 2.21
select user_id,
       if(
         max(days) > datediff(
           date("2021-10-10"), max(login_date)
                     ), max(days), datediff(
           date("2021-10-10"), max(login_date)
                                   )
       )
from (select user_id,
             login_date,
             datediff(
               login_date, lag(login_date) over (
                 partition by
                     user_id
                 order by login_date
                 )
             ) days
      from (select distinct user_id,
                            date(
                              login_ts) login_date
            from user_login_detail) t1) t2
group by user_id;

-- 2.22 答案存疑
select distinct user_id
from (select user_id,
             log_time,
             sum(flag) over (
                 partition by
                     user_id
                 order by log_time
                 ) flag2
      from (select user_id,
                   login_ts log_time,
                   1        flag
            from user_login_detail
            union
            select user_id,
                   logout_ts,
                   -1
            from user_login_detail) t1) t2
where flag2 > 1;

-- 2.23
select distinct sku_id
from (select sku_id,
             month_date - lag(month_date) over (
                 partition by
                     sku_id
                 order by month_date
                 ) flag2
      from (select sku_id,
                   month_date,
                   flag
            from (select sku_id,
                         month(
                           create_date) month_date,
                         if(
                           sum(
                             price * sku_num) > if(
                             sku_id = 1, 21000, 10000), 1, 0
                         )              flag
                  from order_detail
                  where sku_id in ('1', '2')
                  group by sku_id, month(create_date)) t1
            where flag = 1) t2) t2
where flag2 = 1;

-- 2.25
select sku_id,
       category_id
from (select t1.sku_id,
             t1.sum_sku,
             sku_info.category_id,
             rank() over (
                 partition by
                     sku_info.category_id
                 order by t1.sum_sku desc
                 ) rank
      from (select sku_id,
                   sum(sku_num) sum_sku
            from order_detail
            group by sku_id) t1
      join sku_info
        on t1.sku_id = sku_info.sku_id) t2
where rank <= 3;

-- 2.26
select category_id,
       avg(price)
from (select category_id,
             price,
             sum(1) over (
                 partition by
                     category_id
                 ) / 2 avg_num,
             row_number() over (
                 partition by
                     category_id
                 order by price
                 )     number
      from sku_info) t1
where number in (
                 round(avg_num),
                 floor(avg_num + 1)
    )
group by category_id;

-- 2.27
select distinct sku_id
from (select sku_id,
             c_date,
             sum(1) over (
                 partition by
                     sku_id
                 order by c_date range between 2 preceding
                     and current row
                 ) num2
      from (select sku_id,
                   date(create_date)    c_date,
                   sum(sku_num * price) num1
            from order_detail
            group by sku_id, create_date
            having num1
                       > 100) t1) t2
where num2 = 3;

-- 2.29 答案存疑
select sku_id,
       min(c_date),
       max(c_date)
from (select sku_id,
             c_date,
             date_sub(
               c_date, row_number() over (
                 partition by
                     sku_id
                 order by c_date
                 )
             ) flag1
      from (select distinct sku_id,
                            date(
                              create_date) c_date
            from order_detail) t1) t2
group by sku_id,
         flag1;

-- 2.33
select sku_id,
       sub_price
from (select sku_id,
             change_date,
             new_price - lag(new_price, 1, null) over (
                 partition by
                     sku_id
                 order by change_date
                 ) sub_price,
             last_value(change_date) over (
                 partition by
                     sku_id
                 order by change_date rows between unbounded preceding
                     and unbounded following
                 ) last_date
      from sku_price_modify_detail) t1
where change_date = last_date
order by sub_price;

-- 2.34
select distinct user_id,
                first_value(create_date) over (
                    partition by
                        user_id
                    order by create_date rows between unbounded preceding
                        and unbounded following
                    ),
                last_value(create_date) over (
                    partition by
                        user_id
                    order by create_date rows between unbounded preceding
                        and unbounded following
                    ),
                sum_num
from (select order_info.user_id,
             order_detail.create_date,
             sum(1) over (
                 partition by
                     order_info.user_id
                 ) sum_num
      from order_detail
      join order_info
        on order_detail.order_id = order_info.order_id
      where sku_id in (select sku_id
                       from sku_info
                       where name in (
                                      'xiaomi 10', 'apple 12', 'xiaomi 13'
                           ))) t1
where sum_num > 1;

-- 2.37
select id,
       count(*)
from (select case
                 when days1 <= 7 then "新晋用户"
                 when days2 <= 7 then "忠实用户"
                 when days2 <= 30 then "沉睡用户"
                 else "流失用户"
                 end id
      from (select user_id,
                   datediff(
                     max(max_date) over (), min_date
                   ) days1,
                   datediff(
                     max(max_date) over (), max_date
                   ) days2
            from (select user_id,
                         min(date(login_ts)) min_date,
                         max(date(login_ts)) max_date
                  from user_login_detail
                  group by user_id) t1) t2) t3
group by id;

-- 2.38
select user_id,
       sum(coin) coin_num
from (select user_id,
             case days % 7
                 when 3 then 3
                 when 0 then 6
                 else 1
                 end coin
      from (select user_id,
                   rank() over (
                       partition by
                           user_id, flag
                       order by l_date
                       ) days
            from (select user_id,
                         l_date,
                         sum(sub) over (
                             partition by
                                 user_id
                             order by l_date
                             ) flag
                  from (select user_id,
                               l_date,
                               if(
                                 datediff(
                                   l_date, lag(l_date) over (
                                     partition by
                                         user_id
                                     order by l_date
                                     )
                                 ) = 1, 0, 1
                               ) sub
                        from (select distinct user_id,
                                              date(login_ts) l_date
                              from user_login_detail) t1) t2) t3) t4) t5
group by user_id
order by coin_num desc;