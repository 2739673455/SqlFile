-- 1.1各渠道流量统计
DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';
-- 数据装载
-- 方式一
insert overwrite table ads_traffic_stats_by_channel
select *
from ads_traffic_stats_by_channel
union
select
    '2022-06-08',
    *
from (
     select
         1                                                                   as recent_days,
         channel,
         count(distinct mid_id)                                              as uv_count,
         bigint(avg(during_time_1d) / 1000)                                  as avg_duration_sec,
         bigint(avg(page_count_1d))                                          as avg_page_count,
         count(*)                                                            as sv_count,
         cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as DECIMAL(16, 2)) as bounce_rate
     from dws_traffic_session_page_view_1d
     where dt = '2022-06-08'
     group by channel
     union all
     select
         7                                                                   as recent_days,
         channel,
         count(distinct mid_id)                                              as uv_count,
         bigint(avg(during_time_1d) / 1000)                                  as avg_duration_sec,
         bigint(avg(page_count_1d))                                          as avg_page_count,
         count(*)                                                            as sv_count,
         cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as DECIMAL(16, 2)) as bounce_rate
     from dws_traffic_session_page_view_1d
     where date_sub('2022-06-08', 7 - 1) <= dt
       and dt <= '2022-06-08'
     group by channel
     union all
     select
         30                                                                  as recent_days,
         channel,
         count(distinct mid_id)                                              as uv_count,
         bigint(avg(during_time_1d) / 1000)                                  as avg_duration_sec,
         bigint(avg(page_count_1d))                                          as avg_page_count,
         count(*)                                                            as sv_count,
         cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as DECIMAL(16, 2)) as bounce_rate
     from dws_traffic_session_page_view_1d
     where date_sub('2022-06-08', 30 - 1) <= dt
       and dt <= '2022-06-08'
     group by channel
     ) as t1;
-- 方式二
insert overwrite table ads_traffic_stats_by_channel
select *
from ads_traffic_stats_by_channel
union
select
    '2022-06-08',
    bigint(contents['recent_days'])                 as recent_days,
    contents['channel']                             as channel,
    bigint(contents['uv_count'])                    as uv_count,
    bigint(contents['avg_duration_sec'])            as avg_duration_sec,
    bigint(contents['avg_page_count'])              as avg_page_count,
    bigint(contents['sv_count'])                    as sv_count,
    cast(contents['bounce_rate'] as DECIMAL(16, 2)) as bounce_rate
from (
     select
         array(
           map(
             'recent_days', 1,
             'channel', channel,
             'uv_count', count(distinct if(dt = '2022-06-08', mid_id, null)),
             'avg_duration_sec', bigint(avg(if(dt = '2022-06-08', during_time_1d, null)) / 1000),
             'avg_page_count', bigint(avg(if(dt = '2022-06-08', page_count_1d, null))),
             'sv_count', count(if(dt = '2022-06-08', session_id, null)),
             'bounce_rate', cast(sum(if(dt = '2022-06-08' and page_count_1d = 1, 1, 0)) /
                                 count(if(dt = '2022-06-08', session_id, null)) as DECIMAL(16, 2))
           ),
           map(
             'recent_days', 7,
             'channel', channel,
             'uv_count', count(distinct if(date_sub('2022-06-08', 7 - 1) <= dt, mid_id, null)),
             'avg_duration_sec', bigint(avg(if(date_sub('2022-06-08', 7 - 1) <= dt, during_time_1d, null)) / 1000),
             'avg_page_count', bigint(avg(if(date_sub('2022-06-08', 7 - 1) <= dt, page_count_1d, null))),
             'sv_count', count(if(date_sub('2022-06-08', 7 - 1) <= dt, session_id, null)),
             'bounce_rate', cast(sum(if(date_sub('2022-06-08', 7 - 1) <= dt and page_count_1d = 1, 1, 0)) /
                                 count(if(date_sub('2022-06-08', 7 - 1) <= dt, session_id, null)) as DECIMAL(16, 2))
           ),
           map(
             'recent_days', 30,
             'channel', channel,
             'uv_count', count(distinct mid_id),
             'avg_duration_sec', bigint(avg(during_time_1d) / 1000),
             'avg_page_count', bigint(avg(page_count_1d)),
             'sv_count', count(*),
             'bounce_rate', cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as DECIMAL(16, 2))
           )
         ) as content
     from dws_traffic_session_page_view_1d
     where date_sub('2022-06-08', 30 - 1) <= dt
       and dt <= '2022-06-08'
     group by channel
     ) t1 lateral view explode(content) lv as contents;
-- 方式三
--     1. 将数据炸裂成多份，应用在多个场景中
--         将最大范围的数据进行炸裂处理
--         根据炸裂标记对数据进行筛选过滤
--     2. 根据炸裂的结果，将数据进行分组聚合
insert overwrite table ads_traffic_stats_by_channel
select *
from ads_traffic_stats_by_channel
union
select
    '2022-06-08',
    days                                                                as recent_days,
    channel,
    count(distinct mid_id)                                              as uv_count,
    bigint(avg(during_time_1d) / 1000)                                  as avg_duration_sec,
    bigint(avg(page_count_1d))                                          as avg_page_count,
    count(*)                                                            as sv_count,
    cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as DECIMAL(16, 2)) as bounce_rate
from (
     select
         mid_id,
         brand,
         channel,
         during_time_1d,
         page_count_1d,
         dt
     from dws_traffic_session_page_view_1d
     where date_sub('2022-06-08', 30 - 1) <= dt
       and dt <= '2022-06-08'
     ) t lateral view explode(array(1, 7, 30)) lv as days
where date_sub('2022-06-08', days - 1) <= dt
group by days,
         channel;

-- 1.2路径分析
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`         STRING COMMENT '统计日期',
    `source`     STRING COMMENT '跳转起始页面ID',
    `target`     STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_page_path/';
-- 数据装载
-- SQL实现的问题
--     图表工具中不需要考虑第一个页面
--     图表工具中需要考虑最后一个页面
--         将跳转的页面作为跳转页面的source，将连续跳转页面的下一页作为target
--     图表工具中不允许出现环状跳转
insert overwrite table ads_page_path
select *
from ads_page_path
union
select
    '2022-06-08',
    source,
    target,
    count(*)
from (
     select
         concat('step-', rk, ':', page_id)       as source,
         concat('step-', rk + 1, ':', next_page) as target
     from (
          select
              page_id,
              lead(page_id, 1, 'out') over ( partition by session_id order by view_time ) as next_page,
              row_number() over ( partition by session_id order by view_time )            as rk
          from dwd_traffic_page_view_inc
          where dt = '2022-06-08'
          ) t1
     ) t2
group by source,
         target;

-- 2.1用户变动统计
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_change/';
-- 数据装载
insert overwrite table ads_user_change
select *
from ads_user_change
union
select
    '2022-06-08' as dt,
    sum(churn)   as user_churn_count,
    sum(back)    as user_back_count
from (
     select
         if(dt = '2022-06-08' and login_date_last = date_sub('2022-06-08', 7), 1, 0)             as churn,
         if(login_date_last = '2022-06-08' and
            datediff(login_date_last, lag(login_date_last)
                                          over ( partition by user_id order by dt )) >= 8, 1, 0) as back
     from dws_user_user_login_td
     where dt in ('2022-06-08', date_sub('2022-06-08', 1))
     ) t1;

-- 2.2用户留存率
--     新增留存: 今日注册的用户多少后续登录
--     活跃留存: 今日登录的用户多少后续登录
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_retention/';
-- 数据装载
insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select
    '2022-06-08'                                                                          as dt,
    login_date_first                                                                      as create_date,
    datediff('2022-06-08', login_date_first)                                              as retention_day,
    count(if(login_date_last = '2022-06-08', 1, null))                                    as retention_count,
    count(*)                                                                              as new_user_count,
    cast(count(if(login_date_last = '2022-06-08', 1, null)) / count(*) as DECIMAL(16, 2)) as retention_rate
from dws_user_user_login_td
where dt = '2022-06-08'
  and date_sub('2022-06-08', 7) <= login_date_first
  and login_date_first < '2022-06-08'
group by login_date_first;

-- 2.3用户新增活跃统计
DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_stats/';
-- 数据装载
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select
    '2022-06-08'                                                        as dt,
    days                                                                as recent_days,
    sum(if(datediff('2022-06-08', login_date_first) <= days - 1, 1, 0)) as new_user_count,
    count(*)                                                            as active_user_count
from (
     select
         user_id,
         login_date_last,
         login_date_first
     from dws_user_user_login_td
     where dt = '2022-06-08'
       and login_date_last >= date_sub('2022-06-08', 30 - 1)
     ) t1 lateral view explode(array(1, 7, 30)) lv as days
where datediff('2022-06-08', login_date_last) <= days - 1
group by days;

-- 2.4用户行为漏斗分析
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_action/';
-- 数据装载
insert overwrite table ads_user_action
select *
from ads_user_action
union
select
    '2022-06-08',
    `home_count`,
    `good_detail_count`,
    `cart_count`,
    `order_count`,
    `payment_count`
from (
     select
         count(distinct if(page_id = 'home', mid_id, null))        as home_count,
         count(distinct if(page_id = 'good_detail', mid_id, null)) as good_detail_count
     from dws_traffic_page_visitor_page_view_1d
     where dt = '2022-06-08'
       and page_id in ('home', 'good_detail')
     ) as page
join (
     select
         count(*) as cart_count
     from dws_trade_user_cart_add_1d
     where dt = '2022-06-08'
     ) as cart on true
join (
     select
         count(*) as order_count
     from dws_trade_user_order_1d
     where dt = '2022-06-08'
     ) as ordertable on true
join (
     select
         count(*) as payment_count
     from dws_trade_user_payment_1d
     where dt = '2022-06-08'
     ) as pay on true;

-- 2.5新增下单用户统计
DROP TABLE IF EXISTS ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads_new_order_user_stats
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';
-- 数据装载
insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select
    '2022-06-08'   as dt,
    days           as recent_days,
    count(user_id) as new_order_user_count
from (
     select
         user_id,
         order_date_first
     from dws_trade_user_order_td
     where dt = '2022-06-08'
       and date_sub('2022-06-08', 30 - 1) <= order_date_first
       and order_date_first <= '2022-06-08'
     ) t1 lateral view explode(array(1, 7, 30)) lv as days
where datediff('2022-06-08', order_date_first) <= days - 1
group by days;

-- 2.6最近7日内连续3日下单用户数
DROP TABLE IF EXISTS ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads_order_continuously_user_count
(
    `dt`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_order_continuously_user_count/';
-- 数据装载
insert overwrite table ads_order_continuously_user_count
select *
from ads_order_continuously_user_count
union
select
    '2022-06-08'            as dt,
    7                       as recent_days,
    count(distinct user_id) as order_continuously_user_count
from (
     select
         user_id,
         dt,
         sum(1) over ( partition by user_id order by date(dt) range between 2 preceding and current row ) as flag
     from dws_trade_user_order_1d
     where date_sub('2022-06-08', 7 - 1) <= dt
       and dt <= '2022-06-08'
     ) t1
where flag = 3;

-- 3.1最近30日各品牌复购率
DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';
-- 数据装载
insert overwrite table ads_repeat_purchase_by_tm
select *
from ads_repeat_purchase_by_tm
union
select
    '2022-06-08',
    30,
    tm_id,
    tm_name,
    cast(sum(two) / count(*) as decimal(16, 2))
from (
     select
         user_id,
         tm_id,
         tm_name,
         if(sum(order_count_30d) > 1, 1, 0) as two
     from dws_trade_user_sku_order_nd
     group by user_id, tm_id, tm_name
     ) t1
group by tm_id, tm_name;

-- 3.2各个品牌商品下单统计
DROP TABLE IF EXISTS ads_order_stats_by_tm;
CREATE EXTERNAL TABLE ads_order_stats_by_tm
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`            STRING COMMENT '品牌ID',
    `tm_name`          STRING COMMENT '品牌名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_order_stats_by_tm/';
-- 数据装载
insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select
    '2022-06-08'            as dt,
    days                    as recent_days,
    tm_id,
    tm_name,
    sum(order_cn)           as order_count,
    count(distinct user_id) as order_user_count
from (
     select
         dt,
         user_id,
         tm_id,
         tm_name,
         sum(order_count_1d) as order_cn
     from dws_trade_user_sku_order_1d
     where date_sub('2022-06-08', 30 - 1) <= dt
       and dt <= '2022-06-08'
     group by dt, user_id, tm_id, tm_name
     ) t1 lateral view explode(array(1, 7, 30)) lv as days
where datediff('2022-06-08', dt) <= days - 1
group by days, tm_id, tm_name
order by recent_days, tm_id;

-- 3.3各品类商品下单统计
DROP TABLE IF EXISTS ads_order_stats_by_cate;
CREATE EXTERNAL TABLE ads_order_stats_by_cate
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`     STRING COMMENT '一级品类ID',
    `category1_name`   STRING COMMENT '一级品类名称',
    `category2_id`     STRING COMMENT '二级品类ID',
    `category2_name`   STRING COMMENT '二级品类名称',
    `category3_id`     STRING COMMENT '三级品类ID',
    `category3_name`   STRING COMMENT '三级品类名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_order_stats_by_cate/';
-- 数据装载
insert overwrite table ads_order_stats_by_cate
select *
from ads_order_stats_by_cate
union
select
    '2022-06-08'            as dt,
    days                    as recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_cn)           as order_count,
    count(distinct user_id) as order_user_count
from (
     select
         dt,
         user_id,
         category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name,
         sum(order_count_1d) as order_cn
     from dws_trade_user_sku_order_1d
     where date_sub('2022-06-08', 30 - 1) <= dt
       and dt <= '2022-06-08'
     group by dt,
              user_id,
              category1_id,
              category1_name,
              category2_id,
              category2_name,
              category3_id,
              category3_name
     ) t1 lateral view explode(array(1, 7, 30)) lv as days
where datediff('2022-06-08', dt) <= days - 1
group by days,
         category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name
order by recent_days,
         category3_id;

-- 3.4各品类商品购物车存量Top3
DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';
-- 数据装载
insert overwrite table ads_sku_cart_num_top3_by_cate
select *
from ads_sku_cart_num_top3_by_cate
union
select *
from (
     select
         '2022-06-08',
         *,
         rank() over (partition by category3_id order by cart_num desc) as rk
     from (
          select
              category1_id,
              category1_name,
              category2_id,
              category2_name,
              category3_id,
              category3_name,
              cart_add.sku_id,
              sku_name,
              cart_num
          from (
               select
                   sku_id,
                   sum(sku_num) as cart_num
               from dwd_trade_cart_add_inc
               where dt = '2022-06-08'
               group by sku_id
               ) as cart_add
          left join (
                    select
                        id as sku_id,
                        sku_name,
                        category3_id,
                        category3_name,
                        category2_id,
                        category2_name,
                        category1_id,
                        category1_name
                    from dim_sku_full
                    where dt = '2022-06-08'
                    ) as sku_info
            on cart_add.sku_id = sku_info.sku_id
          ) t1
     ) t2
where rk <= 3;

-- 3.5各品牌商品收藏次数Top3
DROP TABLE IF EXISTS ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/';
-- 数据装载
insert overwrite table ads_sku_favor_count_top3_by_tm
select *
from ads_sku_favor_count_top3_by_tm
union
select
    '2022-06-08',
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_count,
    rk
from (
     select *,
            rank() over (partition by tm_id order by favor_count desc) as rk
     from (
          select
              tm_id,
              tm_name,
              sku_id,
              sku_name,
              sum(favor_add_count_1d) as favor_count
          from dws_interaction_sku_favor_add_1d
          where dt = '2022-06-08'
          group by tm_id, tm_name, sku_id, sku_name
          ) t1
     ) t2
where rk <= 3;

-- 4.1下单到支付时间间隔平均值
DROP TABLE IF EXISTS ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE ads_order_to_pay_interval_avg
(
    `dt`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_to_pay_interval_avg/';
-- 数据装载
insert overwrite table ads_order_to_pay_interval_avg
select *
from ads_order_to_pay_interval_avg
union
select
    '2022-06-08',
    bigint(avg(to_unix_timestamp(payment_time) - to_unix_timestamp(order_time)))
from dwd_trade_trade_flow_acc
where dt in ('2022-06-08', '9999-12-31')
  and payment_date_id = '2022-06-08';

-- 4.2各省份交易统计
DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province
(
    `dt`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_code_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各省份交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_by_province/';
-- 数据装载
insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select
    '2022-06-08',
    1,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where dt = '2022-06-08'
union all
select
    '2022-06-08',
    days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    if(days = 7, order_count_7d, order_count_30d),
    if(days = 7, order_total_amount_7d, order_total_amount_30d)
from (
     select
         province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2,
         order_count_7d,
         order_count_30d,
         order_total_amount_7d,
         order_total_amount_30d
     from dws_trade_province_order_nd
     where dt = '2022-06-08'
     ) t1
    lateral view explode(array(7, 30)) lv as days;

-- 5.1优惠券使用统计
DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`              STRING COMMENT '统计日期',
    `coupon_id`       STRING COMMENT '优惠券ID',
    `coupon_name`     STRING COMMENT '优惠券名称',
    `used_count`      BIGINT COMMENT '使用次数',
    `used_user_count` BIGINT COMMENT '使用人数'
) COMMENT '优惠券使用统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';
-- 数据装载
insert overwrite table ads_coupon_stats
select *
from ads_coupon_stats
union
select
    '2022-06-08',
    coupon_id,
    coupon_name,
    sum(used_count_1d) as used_count,
    count(*)           as used_user_count
from dws_tool_user_coupon_coupon_used_1d
where dt = '2022-06-08'
group by coupon_id, coupon_name;