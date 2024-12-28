set
    hive.exec.mode.local.auto = true;

-- 2.7
select t2.sku_id, sku_info.name, sum_sku_num
from (select t1.sku_id, sum(sku_num) sum_sku_num
      from (select sku_id, sku_num
            from order_detail
            where year(create_date) = '2021') t1
      join sku_info
        on sku_info.sku_id = t1.sku_id
      where datediff(
              '2022-01-10', sku_info.from_date
            ) >= 30
      group by t1.sku_id
      having sum(sku_num) < 100) t2
join sku_info
  on sku_info.sku_id = t2.sku_id;

-- 2.11
select t1.user_id,
       t1.register_date,
       t1.total_login,
       t2.total_login_2021,
       t3.order_count_2021,
       t3.order_amount_2021
from (select user_id,
             min(date(login_ts)) register_date,
             count(*)            total_login
      from user_login_detail
      group by user_login_detail.user_id) t1
join (select user_id, count(*) total_login_2021
      from user_login_detail
      where year(login_ts) = '2021'
      group by user_id) t2
  on t1.user_id = t2.user_id
join (select user_id,
             count(order_id)   order_count_2021,
             sum(total_amount) order_amount_2021
      from order_info
      where year(create_date) = '2021'
      group by user_id) t3
  on t1.user_id = t3.user_id;

-- 2.18
-- 方式1
select t4.user_id
from (select user_id
      from (select order_info.user_id, t1.sku_id, t1.order_id
            from order_info
            join (select sku_id, order_id
                  from order_detail
                  where order_detail.sku_id in ('1', '2', '3')) t1
              on order_info.order_id = t1.order_id) t2
      where t2.user_id not in (select order_info.user_id
                               from order_info
                               join (select sku_id, order_id
                                     from order_detail
                                     where order_detail.sku_id = '3') t3
                                 on order_info.order_id = t3.order_id)) t4
group by t4.user_id
having count(*) >= 2;
-- 方式2
select user_id
from (select user_id, collect_set(sku_id) sku_list
      from order_info
      join order_detail
        on order_info.order_id = order_detail.order_id
      group by user_id) t1
where array_contains(t1.sku_list, '1')
  and array_contains(t1.sku_list, '2')
  and ! array_contains(t1.sku_list
    , '3');

-- 2.28
select register_date,
       count(*)   register_count,
       avg(leavl) retention
from (select user_id,
             min(date(login_ts)) register_date,
             if(
               array_contains(
                 collect_list(date(login_ts)), date_add(min(date(login_ts)), 1)
               ), 1, 0
             )                   leavl
      from user_login_detail
      group by user_id) t1
group by register_date;

-- 2.36
select nvl(t1.sku_id, t2.sku_id), nvl(t1.sku_order_count, 0) `购买量`, nvl(t2.sku_favor_count, 0) `收藏量`
from (select sku_id, sum(order_detail.sku_num) sku_order_count
      from order_detail
      where month(order_detail.create_date) = '10'
        and day(order_detail.create_date) <= 7
      group by sku_id) t1
full join (select sku_id, count(*) sku_favor_count
           from favor_info
           where month(favor_info.create_date) = '10'
             and day(favor_info.create_date) <= 7
           group by sku_id) t2
  on t1.sku_id = t2.sku_id;

-- 2.30
select t1.user_id, t1.login_date, t1.login_count, nvl(t2.order_num, 0) order_count
from (select user_id,
             date(login_ts)        login_date,
             count(date(login_ts)) login_count
      from user_login_detail
      group by user_id, date(login_ts)) t1
left join (select user_id, order_date, count(order_date) order_num
           from delivery_info
           group by user_id, order_date) t2
  on t1.user_id = t2.user_id
    and t1.login_date = t2.order_date;

-- 2.8
select t1.register_date, count(*)
from (select min(date(login_ts)) register_date
      from user_login_detail
      group by user_id) t1
group by t1.register_date;

-- 2.14
-- 方式一
select distinct t1.user_id,
                friend_favor.sku_id
from (select user1_id user_id,
             user2_id friend_id
      from friendship_info
      union
      select user2_id, user1_id
      from friendship_info) t1
left join favor_info friend_favor
  on t1.friend_id = friend_favor.user_id
left join favor_info user_favor
  on t1.user_id = user_favor.user_id
    and friend_favor.sku_id = user_favor.sku_id
where user_favor.sku_id is null
order by t1.user_id;
-- 方式二
select distinct user1_id,
                sku_id
from (select user1_id,
             user2_id
      from friendship_info
      union
      select user2_id, user1_id
      from friendship_info) t0
join (select user_id, collect_list(sku_id) favor_set1
      from favor_info
      group by user_id) t1
  on user1_id = t1.user_id
join favor_info
  on user2_id = favor_info.user_id
    and ! array_contains(favor_set1, sku_id)
order by user1_id;
-- 2.16
select t2.create_date,
       sum(
         if(
           t2.gender = '男', t2.sum_amount, 0
         )
       ),
       sum(
         if(
           t2.gender = '女', t2.sum_amount, 0
         )
       )
from (select t1.create_date, t1.user_id, t1.sum_amount, user_info.gender
      from (select create_date,
                   user_id,
                   sum(total_amount) sum_amount
            from order_info
            group by create_date, user_id) t1
      join user_info
        on user_info.user_id = t1.user_id) t2
group by t2.create_date;

-- 2.19
select nvl(
         t1.create_date, t2.create_date
       ),
       nvl(t1.sum1, 0) - nvl(t2.sum2, 0)
from (select create_date, sum(sku_num) sum1
      from order_detail
      where sku_id = '1'
      group by create_date) t1
full join (select create_date, sum(sku_num) sum2
           from order_detail
           where sku_id = '2'
           group by create_date) t2
  on t1.create_date = t2.create_date;

-- 2.24
select t1.cateage, count(*)
from (select sku_id,
             case
                 when sum(sku_num) < 5001 then '冷门商品'
                 when sum(sku_num) between 5001 and 19999 then '一般商品'
                 else '热门商品'
                 end cateage
      from order_detail
      group by sku_id) t1
group by t1.cateage;

-- 2.31
select sku_id, year(create_date) year, sum(sku_num * price) `销售总额`
from order_detail t1
group by sku_id,
         year(create_date);

-- 2.32
select sku_id,
       sum(
         if(
           create_date = date('2021-9-28'),
           sku_num,
           0
         )
       ) mon,
       sum(
         if(
           create_date = date('2021-9-29'),
           sku_num,
           0
         )
       ) tue,
       sum(
         if(
           create_date = date('2021-9-30'),
           sku_num,
           0
         )
       ) wed,
       sum(
         if(
           create_date = date('2021-10-1'),
           sku_num,
           0
         )
       ) thu,
       sum(
         if(
           create_date = date('2021-10-2'),
           sku_num,
           0
         )
       ) fri,
       sum(
         if(
           create_date = date('2021-10-3'),
           sku_num,
           0
         )
       ) sat,
       sum(
         if(
           create_date = date('2021-9-27'),
           sku_num,
           0
         )
       ) sun
from order_detail
where create_date between date('2021-9-27')
          and date('2021-10-3')
group by sku_id;

-- 2.35
select sku_id,
       month(create_date) month,
       sum(
         if(
           year(create_date) = 2020, sku_num, 0
         )
       )                  `2020`,
       sum(
         if(
           year(create_date) = 2021, sku_num, 0
         )
       )                  `2021`
from order_detail
where year(create_date) in (2020, 2021)
group by sku_id,
         month(create_date);

-- 2.39
select t1.category_id,
       sum(if(create_date = date('2021-10-1'), t1.sale_rate, 0))   1_sale_rate,
       sum(if(create_date = date('2021-10-1'), t1.unsale_rate, 0)) 1_unsale_rate,
       sum(if(create_date = date('2021-10-2'), t1.sale_rate, 0))   2_sale_rate,
       sum(if(create_date = date('2021-10-2'), t1.unsale_rate, 0)) 2_unsale_rate,
       sum(if(create_date = date('2021-10-3'), t1.sale_rate, 0))   3_sale_rate,
       sum(if(create_date = date('2021-10-3'), t1.unsale_rate, 0)) 3_unsale_rate,
       sum(if(create_date = date('2021-10-4'), t1.sale_rate, 0))   4_sale_rate,
       sum(if(create_date = date('2021-10-4'), t1.unsale_rate, 0)) 4_unsale_rate,
       sum(if(create_date = date('2021-10-5'), t1.sale_rate, 0))   5_sale_rate,
       sum(if(create_date = date('2021-10-5'), t1.unsale_rate, 0)) 5_unsale_rate,
       sum(if(create_date = date('2021-10-6'), t1.sale_rate, 0))   6_sale_rate,
       sum(if(create_date = date('2021-10-6'), t1.unsale_rate, 0)) 6_unsale_rate,
       sum(if(create_date = date('2021-10-7'), t1.sale_rate, 0))   7_sale_rate,
       sum(if(create_date = date('2021-10-7'), t1.unsale_rate, 0)) 7_unsale_rate
from (select t1.category_id,
             order_detail.create_date,
             size(collect_set(order_detail.sku_id)) / avg(sum_sku)     sale_rate,
             1 - size(collect_set(order_detail.sku_id)) / avg(sum_sku) unsale_rate
      from order_detail
      join (select category_id, count(*) sum_sku, collect_list(sku_id) sku_list
            from sku_info
            group by category_id) t1
        on array_contains(t1.sku_list, order_detail.sku_id)
      where order_detail.create_date between date('2021-10-1') and date('2021-10-7')
      group by t1.category_id, order_detail.create_date) t1
group by t1.category_id;