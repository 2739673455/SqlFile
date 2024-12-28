-- 1.1交易域用户商品粒度订单最近1日汇总表
--     用户商品粒度: user + sku
--         group by
--     订单: 业务行为
--     不能进行用户的统计，用户字段可以跨越天存在，不能以1d为单位统计
--     1d表中不允许出现可以跨越天的字段统计，可以将跨越天的字段声明在1d表或nd表中，最后单独统计
DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
CREATE
EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `sku_id`                    STRING COMMENT 'SKU_ID',
    `sku_name`                  STRING COMMENT 'SKU名称',
    `category1_id`              STRING COMMENT '一级品类ID',
    `category1_name`            STRING COMMENT '一级品类名称',
    `category2_id`              STRING COMMENT '二级品类ID',
    `category2_name`            STRING COMMENT '二级品类名称',
    `category3_id`              STRING COMMENT '三级品类ID',
    `category3_name`            STRING COMMENT '三级品类名称',
    `tm_id`                     STRING COMMENT '品牌ID',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
set
hive.exec.dynamic.partition.mode=nonstrict;
with dwd_tod as (
    select
        user_id,
        sku_id,
        count(*)                   as order_count_1d,
        sum(sku_num)               as order_num_1d,
        sum(split_original_amount) as order_original_amount_1d,
        sum(split_activity_amount) as activity_reduce_amount_1d,
        sum(split_coupon_amount)   as coupon_reduce_amount_1d,
        sum(split_total_amount)    as order_total_amount_1d,
        dt
    from dwd_trade_order_detail_inc
    group by user_id, sku_id, dt
                )
   , dim_sku as (
    select
        id as sku_id,
        sku_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt = '2022-06-08'
                )
insert
overwrite
table
dws_trade_user_sku_order_1d
partition
(
dt
)
select
    `user_id`,
    dwd_tod.`sku_id`,
    `sku_name`,
    `category1_id`,
    `category1_name`,
    `category2_id`,
    `category2_name`,
    `category3_id`,
    `category3_name`,
    `tm_id`,
    `tm_name`,
    `order_count_1d`,
    `order_num_1d`,
    `order_original_amount_1d`,
    `activity_reduce_amount_1d`,
    `coupon_reduce_amount_1d`,
    `order_total_amount_1d`,
    dt
from dwd_tod
left join (
    select *
    from dim_sku
          ) as dim_sku
  on dwd_tod.sku_id = dim_sku.sku_id;
-- 每日
insert
overwrite table dws_trade_user_sku_order_1d partition (dt = '2022-06-09')
select
    `user_id`,
    dwd_tod.`sku_id`,
    `sku_name`,
    `category1_id`,
    `category1_name`,
    `category2_id`,
    `category2_name`,
    `category3_id`,
    `category3_name`,
    `tm_id`,
    `tm_name`,
    `order_count_1d`,
    `order_num_1d`,
    `order_original_amount_1d`,
    `activity_reduce_amount_1d`,
    `coupon_reduce_amount_1d`,
    `order_total_amount_1d`
from (
    select
        user_id,
        sku_id,
        count(*)                   as order_count_1d,
        sum(sku_num)               as order_num_1d,
        sum(split_original_amount) as order_original_amount_1d,
        sum(split_activity_amount) as activity_reduce_amount_1d,
        sum(split_coupon_amount)   as coupon_reduce_amount_1d,
        sum(split_total_amount)    as order_total_amount_1d
    from dwd_trade_order_detail_inc
    where dt = '2022-06-09'
    group by user_id, sku_id, dt
     ) as dwd_tod
left join (
    select
        id as sku_id,
        sku_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt = '2022-06-09'
          ) as dim_sku
  on dwd_tod.sku_id = dim_sku.sku_id;

-- 1.2交易域用户粒度订单最近1日汇总表
DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE
EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_trade_user_order_1d partition (dt)
select
    user_id,
    count(distinct order_id)   as order_count_1d,
    sum(sku_num)               as order_num_1d,
    sum(split_original_amount) as order_original_amount_1d,
    sum(split_activity_amount) as activity_reduce_amount_1d,
    sum(split_coupon_amount)   as coupon_reduce_amount_1d,
    sum(split_total_amount)    as order_total_amount_1d,
    dt
from dwd_trade_order_detail_inc
where dt = '2022-06-08'
group by user_id, dt;
-- 每日
insert
overwrite table dws_trade_user_order_1d partition (dt = '2022-06-09')
select
    user_id,
    count(distinct order_id)   as order_count_1d,
    sum(sku_num)               as order_num_1d,
    sum(split_original_amount) as order_original_amount_1d,
    sum(split_activity_amount) as activity_reduce_amount_1d,
    sum(split_coupon_amount)   as coupon_reduce_amount_1d,
    sum(split_total_amount)    as order_total_amount_1d
from dwd_trade_order_detail_inc
where dt = '2022-06-09'
group by user_id;

-- 1.3交易域用户粒度加购最近1日汇总表
DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE
EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dws_trade_user_cart_add_1d partition (dt)
select
    user_id,
    count(*),
    sum(sku_num),
    dt
from dwd_trade_cart_add_inc
group by user_id, dt;
-- 每日
insert
overwrite table dws_trade_user_cart_add_1d partition (dt = '2022-06-09')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd_trade_cart_add_inc
where dt = '2022-06-09'
group by user_id;

-- 1.4交易域用户粒度支付最近1日汇总表
DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE
EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_trade_user_payment_1d partition (dt)
select
    user_id,
    count(distinct order_id),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd_trade_pay_detail_suc_inc
group by user_id, dt;
-- 每日
insert
overwrite table dws_trade_user_payment_1d partition (dt = '2022-06-09')
select
    user_id,
    count(distinct order_id),
    sum(sku_num),
    sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
where dt = '2022-06-09'
group by user_id;

-- 1.5交易域省份粒度订单最近1日汇总表
DROP TABLE IF EXISTS dws_trade_province_order_1d;
CREATE
EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '省份ID',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                STRING COMMENT '新版国际标准地区编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_trade_province_order_1d partition (dt)
select
    dwd_trade_order_detail.`province_id`,
    `province_name`,
    `area_code`,
    `iso_code`,
    `iso_3166_2`,
    `order_count_1d`,
    `order_original_amount_1d`,
    `activity_reduce_amount_1d`,
    `coupon_reduce_amount_1d`,
    `order_total_amount_1d`,
    dt
from (
    select
        province_id,
        count(distinct order_id)   as order_count_1d,
        sum(split_original_amount) as order_original_amount_1d,
        sum(split_activity_amount) as activity_reduce_amount_1d,
        sum(split_coupon_amount)   as coupon_reduce_amount_1d,
        sum(split_total_amount)    as order_total_amount_1d,
        dt
    from dwd_trade_order_detail_inc
    group by province_id, dt
     ) dwd_trade_order_detail
left join (
    select
        id as province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_full
    where dt = '2022-06-08'
          ) dim_province
  on dwd_trade_order_detail.province_id = dim_province.province_id;
-- 每日
insert
overwrite table dws_trade_province_order_1d partition (dt = '2022-06-09')
select
    dwd_trade_order_detail.`province_id`,
    `province_name`,
    `area_code`,
    `iso_code`,
    `iso_3166_2`,
    `order_count_1d`,
    `order_original_amount_1d`,
    `activity_reduce_amount_1d`,
    `coupon_reduce_amount_1d`,
    `order_total_amount_1d`
from (
    select
        province_id,
        count(distinct order_id)   as order_count_1d,
        sum(split_original_amount) as order_original_amount_1d,
        sum(split_activity_amount) as activity_reduce_amount_1d,
        sum(split_coupon_amount)   as coupon_reduce_amount_1d,
        sum(split_total_amount)    as order_total_amount_1d
    from dwd_trade_order_detail_inc
    where dt = '2022-06-09'
    group by province_id
     ) dwd_trade_order_detail
left join (
    select
        id as province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_full
    where dt = '2022-06-09'
          ) dim_province
  on dwd_trade_order_detail.province_id = dim_province.province_id;

-- 1.6工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表
DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
CREATE
EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
(
    `user_id`          STRING COMMENT '用户ID',
    `coupon_id`        STRING COMMENT '优惠券ID',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `benefit_rule`     STRING COMMENT '优惠规则',
    `used_count_1d`    STRING COMMENT '使用(支付)次数'
) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt)
select
    user_id,
    used.coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count_1d,
    dt
from (
    select
        user_id,
        coupon_id,
        count(*) as used_count_1d,
        dt
    from dwd_tool_coupon_used_inc
    group by dt, user_id, coupon_id
     ) as used
left join (
    select
        id as coupon_id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where dt = '2022-06-08'
          ) coupon
  on used.coupon_id = coupon.coupon_id;
-- 每日
insert
overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt = '2022-06-09')
select
    user_id,
    used.coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count_1d
from (
    select
        user_id,
        coupon_id,
        count(*) as used_count_1d
    from dwd_tool_coupon_used_inc
    where dt = '2022-06-09'
    group by user_id, coupon_id
     ) as used
left join (
    select
        id as coupon_id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where dt = '2022-06-09'
          ) coupon
  on used.coupon_id = coupon.coupon_id;

-- 1.7互动域商品粒度收藏商品最近1日汇总表
DROP TABLE IF EXISTS dws_interaction_sku_favor_add_1d;
CREATE
EXTERNAL TABLE dws_interaction_sku_favor_add_1d
(
    `sku_id`             STRING COMMENT 'SKU_ID',
    `sku_name`           STRING COMMENT 'SKU名称',
    `category1_id`       STRING COMMENT '一级品类ID',
    `category1_name`     STRING COMMENT '一级品类名称',
    `category2_id`       STRING COMMENT '二级品类ID',
    `category2_name`     STRING COMMENT '二级品类名称',
    `category3_id`       STRING COMMENT '三级品类ID',
    `category3_name`     STRING COMMENT '三级品类名称',
    `tm_id`              STRING COMMENT '品牌ID',
    `tm_name`            STRING COMMENT '品牌名称',
    `favor_add_count_1d` BIGINT COMMENT '商品被收藏次数'
) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_interaction_sku_favor_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dws_interaction_sku_favor_add_1d partition (dt)
select
    favor.sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count_1d,
    dt
from (
    select
        sku_id,
        dt,
        count(*) as favor_add_count_1d
    from dwd_interaction_favor_add_inc
    group by dt, sku_id
     ) as favor
left join (
    select
        id as sku_id,
        sku_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt = '2022-06-08'
          ) as sku
  on favor.sku_id = sku.sku_id;
-- 每日
insert
overwrite table dws_interaction_sku_favor_add_1d partition (dt = '2022-06-09')
select
    favor.sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count_1d
from (
    select
        sku_id,
        count(*) as favor_add_count_1d
    from dwd_interaction_favor_add_inc
    where dt = '2022-06-09'
    group by sku_id
     ) as favor
left join (
    select
        id as sku_id,
        sku_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt = '2022-06-09'
          ) as sku
  on favor.sku_id = sku.sku_id;

-- 1.8流量域会话粒度页面浏览最近1日汇总表
DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE
EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话ID',
    `mid_id`         string comment '设备ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'APP版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `page_count_1d`  BIGINT COMMENT '最近1日浏览页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert
overwrite table dws_traffic_session_page_view_1d partition (dt = '2022-06-08')
select
    `session_id`,
    `mid_id`,
    `brand`,
    `model`,
    `operate_system`,
    `version_code`,
    `channel`,
    sum(during_time) as `during_time_1d`,
    count(*)         as `page_count_1d`
from dwd_traffic_page_view_inc
where dt = '2022-06-08'
group by `session_id`,
         `mid_id`,
         `brand`,
         `model`,
         `operate_system`,
         `version_code`,
         `channel`;

-- 1.9流量域访客页面粒度页面浏览最近1日汇总表
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE
EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面ID',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert
overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2022-06-08')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt = '2022-06-08'
group by mid_id, brand, model, operate_system, page_id;

-- 2.1交易域用户商品粒度订单最近n日汇总表
DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE
EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户ID',
    `sku_id`                     STRING COMMENT 'SKU_ID',
    `sku_name`                   STRING COMMENT 'SKU名称',
    `category1_id`               STRING COMMENT '一级品类ID',
    `category1_name`             STRING COMMENT '一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID',
    `category2_name`             STRING COMMENT '二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID',
    `category3_name`             STRING COMMENT '三级品类名称',
    `tm_id`                      STRING COMMENT '品牌ID',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert
overwrite table dws_trade_user_sku_order_nd partition (dt = '2022-06-08')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_count_1d, 0))            as order_count_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_num_1d, 0))              as order_num_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_original_amount_1d, 0))  as order_original_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, coupon_reduce_amount_1d, 0))   as coupon_reduce_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_total_amount_1d, 0))     as order_total_amount_7d,
    sum(order_count_1d)                                                    as order_count_30d,
    sum(order_num_1d)                                                      as order_num_30d,
    sum(order_original_amount_1d)                                          as order_original_amount_30d,
    sum(activity_reduce_amount_1d)                                         as activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d)                                           as coupon_reduce_amount_30d,
    sum(order_total_amount_1d)                                             as order_total_amount_30d
from dws_trade_user_sku_order_1d
where date_sub('2022-06-08', 29) <= dt
  and dt <= '2022-06-08'
group by user_id,
         sku_id,
         sku_name,
         category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name,
         tm_id,
         tm_name;

-- 2.2交易域省份粒度订单最近n日汇总表
DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE
EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_trade_province_order_nd partition (dt = '2022-06-08')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_count_1d, 0))            as order_count_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_original_amount_1d, 0))  as order_original_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, coupon_reduce_amount_1d, 0))   as coupon_reduce_amount_7d,
    sum(if(date_sub('2022-06-08', 6) <= dt, order_total_amount_1d, 0))     as order_total_amount_7d,
    sum(order_count_1d)                                                    as order_count_30d,
    sum(order_original_amount_1d)                                          as order_original_amount_30d,
    sum(activity_reduce_amount_1d)                                         as activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d)                                           as coupon_reduce_amount_30d,
    sum(order_total_amount_1d)                                             as order_total_amount_30d
from dws_trade_province_order_1d
where date_sub('2022-06-08', 29) <= dt
  and dt <= '2022-06-08'
group by province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2;

-- 3.1交易域用户粒度订单历史至今汇总表
DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE
EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `order_num_td`              BIGINT COMMENT '历史至今购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '历史至今下单活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日: 查询全部数据
insert
overwrite table dws_trade_user_order_td partition (dt = '2022-06-08')
select
    user_id,
    min(dt),
    max(dt),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_order_1d
group by user_id;
-- 每日: 昨日数据 + 今日数据 => 进一步聚合
insert
overwrite table dws_trade_user_order_td partition (dt = '2022-06-09')
select
    user_id,
    min(order_date_first),
    max(order_date_last),
    sum(order_count_td),
    sum(order_num_td),
    sum(original_amount_td),
    sum(activity_reduce_amount_td),
    sum(coupon_reduce_amount_td),
    sum(total_amount_td)
from (
    select
        user_id,
        dt                        as order_date_first,
        dt                        as order_date_last,
        order_count_1d            as order_count_td,
        order_num_1d              as order_num_td,
        order_original_amount_1d  as original_amount_td,
        activity_reduce_amount_1d as activity_reduce_amount_td,
        coupon_reduce_amount_1d   as coupon_reduce_amount_td,
        order_total_amount_1d     as total_amount_td,
        dt
    from dws_trade_user_order_1d
    where dt = '2022-06-09'
    union all
    select *
    from dws_trade_user_order_td
    where dt = date_sub('2022-06-09', 1)
     ) t1
group by user_id;

-- 3.2用户域用户粒度登录历史至今汇总表
--     电商系统中，一般认为用户注册成功就是首次登录成功
--     用户登录历史至今表可以考虑从注册用户行为中进行数据处理
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE
EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`          STRING COMMENT '用户ID',
    `login_date_last`  STRING COMMENT '历史至今末次登录日期',
    `login_date_first` STRING COMMENT '历史至今首次登录日期',
    `login_count_td`   BIGINT COMMENT '历史至今累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert
overwrite table dws_user_user_login_td partition (dt = '2022-06-08')
select
    user_id,
    max(dt)  as login_date_last,
    min(dt)  as login_date_first,
    count(*) as login_count_td
from dwd_user_login_inc
group by user_id;
-- 从注册用户行为中进行数据处理
insert
overwrite table dws_user_user_login_td partition (dt = '2022-06-08')
select
    user_id,
    max(login_date_last),
    min(login_date_first),
    sum(login_count_td)
from (
    select
        user_id,
        dt as login_date_last,
        dt as login_date_first,
        1  as login_count_td
    from dwd_user_register_inc
    union all
    select
        user_id,
        '2022-06-08',
        '2022-06-08',
        count(*)
    from dwd_user_login_inc
    group by user_id
     ) t1
group by user_id;
-- 从注册用户行为中进行数据处理(拉链表)
insert
overwrite table dws_user_user_login_td partition (dt = '2022-06-08')
select
    user_id,
    nvl(max(login_date_last), min(login_date_first)),
    min(login_date_first),
    sum(login_count_td)
from (
    select
        id                                      as user_id,
        date_format(operate_time, 'yyyy-MM-dd') as login_date_last,
        date_format(create_time, 'yyyy-MM-dd')  as login_date_first,
        1                                       as login_count_td
    from dim_user_zip
    where dt = '9999-12-31'
      and date_format(create_time, 'yyyy-MM-dd') < '2022-06-08'
    union all
    select
        user_id,
        '2022-06-08',
        '2022-06-08',
        count(*)
    from dwd_user_login_inc
    group by user_id
     ) t1
group by user_id;
-- 每日
insert
overwrite table dws_user_user_login_td partition (dt = '2022-06-09')
select
    user_id,
    max(login_date_last),
    min(login_date_first),
    sum(login_count_td)
from (
    select
        user_id,
        dt       as login_date_last,
        dt       as login_date_first,
        count(*) as login_count_td,
        dt
    from dwd_user_login_inc
    where dt = '2022-06-09'
    group by user_id, dt
    union all
    select *
    from dws_user_user_login_td
     ) t1
group by user_id;