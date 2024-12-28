-- DWD层: 准备数据(统计) Data Warehouse Detail
--     本质是融合ODS层多张表
--     DWD层保存行为数据
--         ODS层保存的就是状态数据，因为MySQL业务数据库不存行为数据
--         根据状态反推行为
--     DIM层保存状态数据
--     设计要点
--         建表需参考 事实表 设计理论
--         为统计分析做准备: ORC,snappy
--         数据量大: 分区表
--         命名规范: 分层标记(dwd_) + 数据域(包名，分类) + 行为 + 全量/增量(full/inc)
--                 数据仓库的分层架构是逻辑架构，不存在具体的物理文件或文件夹
--                 绝大多数的事实表(行为)都是增量表，特殊业务例外

-- 事实表
--     维度字段 + 度量值(用于计算的字段)
--         如果一个行为无法产生用于度量(统计)的值，那就无需创建事实表
--     事实表主要分为三大类:
--         事务事实表
--             绝大多数事实表都是事务事实表
--             增量
--             事务(原子性)
--                 保存的就是原子性的业务行为数据
--                 是否创建独立的事实表取决于独立的行为是否需要统计
--             粒度: 描述行为表中一行数据的详细程度，描述的越详细粒度越细
--                 维度越多，粒度越细
--             创建表的步骤:
--                 1.选择业务过程: 确定表
--                 2.声明粒度: 确定行
--                 3.确认维度: 确定列
--                 4.确认事实: 确定度量值(用于统计的列)
--             表的设计
--                 必要的维度字段 + 度量值 + 可选维度
--         周期快照事实表
--         累计快照事实表

-- 1交易域加购(将商品加入购物车)事实表
--     时间(行为:秒) 用户 商品 数量
drop table if exists dwd_trade_cart_add_inc;
create external table dwd_trade_cart_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '加购时间',
    `sku_num`     BIGINT COMMENT '加购物车件数'
) comment "交易域加购事务事实表" partitioned by (`dt` string)
    stored as orc location '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/' tblproperties ('orc.compress' = 'snappy');

-- 获取的数据是否是用户的一次加购行为: 不能保证
-- 获取的数据是否为同一天的行为数据: 不能保证
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date(data.create_time) as date_id,
    data.create_time,
    data.sku_num,
    date(data.create_time)
from ods_cart_info_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert';
-- 每日
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2022-06-09')
select
    data.`id`,
    data.`user_id`,
    data.`sku_id`,
    date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
    date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as create_time,
    data.`sku_num` - nvl(bigint(old['sku_num']), 0)                            as sku_num
from ods_cart_info_inc
where dt = '2022-06-09'
  and (type = 'insert'
    or (type = 'update'
        and array_contains(map_keys(old), 'sku_num')
        and data.`sku_num` > bigint(old['sku_num'])));

-- 2交易域下单事实表
--     时间 用户 订单 商品 金额 数量
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    odd.id,
    odd.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    odi.create_time,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_id
from (
     select
         data.`id`,
         data.`order_id`,
         data.`sku_id`,
         data.`create_time`,
         data.`sku_num`,
         data.sku_num * data.order_price as `split_original_amount`,
         data.`split_activity_amount`,
         data.`split_coupon_amount`,
         data.`split_total_amount`
     from ods_order_detail_inc
     where dt = '2022-06-08'
       and type = 'bootstrap-insert'
     ) odd
left join(
         select
             data.`id`              as order_id,
             data.`user_id`,
             data.`province_id`,
             date(data.create_time) as date_id,
             data.create_time
         from ods_order_info_inc
         where dt = '2022-06-08'
           and type = 'bootstrap-insert'
         ) odi
  on odd.order_id = odi.order_id
left join(
         select
             data.`order_detail_id` as id,
             data.`activity_id`,
             data.`activity_rule_id`
         from ods_order_detail_activity_inc
         where dt = '2022-06-08'
           and type = 'bootstrap-insert'
         ) oda
  on odd.id = oda.id
left join (
          select
              data.order_detail_id as id,
              data.`coupon_id`
          from ods_order_detail_coupon_inc
          where dt = '2022-06-08'
            and type = 'bootstrap-insert'
          ) odc
  on odd.id = odc.id;
-- 每日
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2022-06-09')
select
    odd.id,
    odd.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    odi.create_time,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from (
     select
         data.`id`,
         data.`order_id`,
         data.`sku_id`,
         data.`create_time`,
         data.`sku_num`,
         data.sku_num * data.order_price as `split_original_amount`,
         data.`split_activity_amount`,
         data.`split_coupon_amount`,
         data.`split_total_amount`
     from ods_order_detail_inc
     where dt = '2022-06-09'
       and type = 'insert'
     ) odd
left join(
         select
             data.`id`              as order_id,
             data.`user_id`,
             data.`province_id`,
             date(data.create_time) as date_id,
             data.create_time
         from ods_order_info_inc
         where dt = '2022-06-09'
           and type = 'insert'
         ) odi
  on odd.order_id = odi.order_id
left join(
         select
             data.`order_detail_id` as id,
             data.`activity_id`,
             data.`activity_rule_id`
         from ods_order_detail_activity_inc
         where dt = '2022-06-09'
           and type = 'insert'
         ) oda
  on odd.id = oda.id
left join (
          select
              data.order_detail_id as id,
              data.`coupon_id`
          from ods_order_detail_coupon_inc
          where dt = '2022-06-09'
            and type = 'insert'
          ) odc
  on odd.id = odc.id;

-- 3交易域支付成功事务事实表
drop table if exists dwd_trade_pay_detail_suc_inc;
create external table dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) comment "交易域支付成功事务事实表"
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    tblproperties ('orc.compress' = 'snappy');
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_payment_amount,
    date_id
from (
     select
         data.id,
         data.order_id,
         data.sku_id,
         data.sku_num,
         data.order_price * data.sku_num as split_original_amount,
         data.split_activity_amount,
         data.split_coupon_amount,
         data.split_total_amount         as split_payment_amount
     from ods_order_detail_inc
     where dt = '2022-06-08'
       and type = 'bootstrap-insert'
     ) od
join (
     select
         data.order_id,
         data.payment_type        as payment_type_code,
         data.callback_time,
         date(data.callback_time) as date_id
     from ods_payment_info_inc
     where dt = '2022-06-08'
       and type = 'bootstrap-insert'
       and data.payment_status = '1602'
     ) py
  on od.order_id = py.order_id
left join (
          select
              data.id as order_id,
              data.user_id,
              data.province_id
          from ods_order_info_inc
          where dt = '2022-06-08'
            and type = 'bootstrap-insert'
          ) oi
  on od.order_id = oi.order_id
left join (
          select
              data.activity_id,
              data.activity_rule_id,
              data.order_detail_id as id
          from ods_order_detail_activity_inc
          where dt = '2022-06-08'
            and type = 'bootstrap-insert'
          ) act
  on od.id = act.id
left join (
          select
              data.coupon_id,
              data.order_detail_id as id
          from ods_order_detail_coupon_inc
          where dt = '2022-06-08'
            and type = 'bootstrap-insert'
          ) cp
  on od.id = cp.id
left join (
          select
              dic_code,
              dic_name as payment_type_name
          from ods_base_dic_full
          where dt = '2022-06-08'
            and parent_code = '11'
          ) dic
  on py.payment_type_code = dic.dic_code;
-- 每日
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2022-06-09')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_payment_amount
from (
     select
         data.id,
         data.order_id,
         data.sku_id,
         data.sku_num,
         data.order_price * data.sku_num as split_original_amount,
         data.split_activity_amount,
         data.split_coupon_amount,
         data.split_total_amount         as split_payment_amount
     from ods_order_detail_inc
     where dt in ('2022-06-09', date_sub('2022-06-09', 1))
       and type in ('insert', 'bootstrap-insert')
     ) od
left join (
          select
              data.id as order_id,
              data.user_id,
              data.province_id
          from ods_order_info_inc
          where dt in ('2022-06-09', date_sub('2022-06-09', 1))
            and type in ('insert', 'bootstrap-insert')
          ) oi
  on od.order_id = oi.order_id
left join (
          select
              data.activity_id,
              data.activity_rule_id,
              data.order_detail_id as id
          from ods_order_detail_activity_inc
          where dt in ('2022-06-09', date_sub('2022-06-09', 1))
            and type in ('insert', 'bootstrap-insert')
          ) act
  on od.id = act.id
left join (
          select
              data.coupon_id,
              data.order_detail_id as id
          from ods_order_detail_coupon_inc
          where dt in ('2022-06-09', date_sub('2022-06-09', 1))
            and type in ('insert', 'bootstrap-insert')
          ) cp
  on od.id = cp.id
join (
     select
         data.order_id,
         data.payment_type        as payment_type_code,
         data.callback_time,
         date(data.callback_time) as date_id
     from ods_payment_info_inc
     where dt = '2022-06-09'
       and type = 'update'
       and array_contains(map_keys(old), 'payment_status')
       and data.payment_status = '1602'
     ) py
  on od.order_id = py.order_id
left join (
          select
              dic_code,
              dic_name as payment_type_name
          from ods_base_dic_full
          where dt = '2022-06-09'
            and parent_code = '11'
          ) dic
  on py.payment_type_code = dic.dic_code;

-- 4交易域购物车周期快照事实表
--     剩余数量概念在业务中非常重要，一般业务中都会设计相应的字段进行保存
--     余额字段会在每次数据操作中进行修改，最后一次修改的数据状态就是余额
drop table if exists dwd_trade_cart_full;
create external table dwd_trade_cart_full
(
    id       STRING COMMENT '编号',
    user_id  STRING COMMENT '用户ID',
    sku_id   STRING COMMENT 'SKU_ID',
    sku_name STRING COMMENT '商品名称',
    sku_num  BIGINT COMMENT '现存商品件数'
) comment '交易域购物车周期快照事实表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    tblproperties ('orc.compress' = 'snappy');
-- 数据载入
insert overwrite table dwd_trade_cart_full partition (dt = '2022-06-08')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full
where dt = '2022-06-08'
  and is_ordered = '0';

-- 5交易域交易流程累计快照事实表
-- 累积型快照事实表
--     将一个流程中多个业务行为的状态数据保存到一张表中
--         目的是统计不同业务行为之间的业务关系
--     分区策略: 以业务时间为基准设计分区，推荐使用流程最后一个业务的时间来分区
--         如果最后一个业务时间为null，使用时间极大值替换
DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    order_id              STRING COMMENT '订单ID',
    user_id               STRING COMMENT '用户ID',
    province_id           STRING COMMENT '省份ID',
    order_date_id         STRING COMMENT '下单日期ID',
    order_time            STRING COMMENT '下单时间',
    payment_date_id       STRING COMMENT '支付日期ID',
    payment_time          STRING COMMENT '支付时间',
    finish_date_id        STRING COMMENT '确认收货日期ID',
    finish_time           STRING COMMENT '确认收货时间',
    order_original_amount DECIMAL(16, 2) COMMENT '下单原始价格',
    order_activity_amount DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    order_coupon_amount   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    order_total_amount    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    payment_amount        DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select
    oii.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    payment_date_id,
    payment_time,
    finish_date_id,
    finish_time,
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    payment_amount,
    nvl(finish_date_id, '9999-12-31')
from (
     select
         data.id                     as order_id,
         data.user_id,
         data.province_id,
         date(data.create_time)      as order_date_id,
         data.create_time            as order_time,
         data.original_total_amount  as order_original_amount,
         data.activity_reduce_amount as order_activity_amount,
         data.coupon_reduce_amount   as order_coupon_amount,
         data.total_amount           as order_total_amount
     from ods_order_info_inc
     where dt = '2022-06-08'
       and type = 'bootstrap-insert'
     ) oii
left join (
          select
              data.order_id,
              data.callback_time       as payment_time,
              date(data.callback_time) as payment_date_id,
              data.total_amount        as payment_amount
          from ods_payment_info_inc
          where dt = '2022-06-08'
            and type = 'bootstrap-insert'
            and data.payment_status = '1602'
          ) pii
  on oii.order_id = pii.order_id
left join(
         select
             data.order_id,
             data.create_time       as finish_time,
             date(data.create_time) as finish_date_id
         from ods_order_status_log_inc
         where dt = '2022-06-08'
           and type = 'bootstrap-insert'
           and data.order_status = '1004'
         ) osl
  on oii.order_id = osl.order_id;
-- 每日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select
    oii.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    nvl(oii.payment_date_id, pii.payment_date_id) as payment_date_id,
    nvl(oii.payment_time, pii.payment_time)       as payment_time,
    nvl(oii.finish_date_id, osl.finish_date_id)   as finish_date_id,
    nvl(oii.finish_time, osl.finish_time)         as finish_time,
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    nvl(oii.payment_amount, pii.payment_amount),
    nvl(nvl(oii.finish_time, osl.finish_time), '9999-12-31')
from (
     select
         order_id,
         user_id,
         province_id,
         order_date_id,
         order_time,
         payment_date_id,
         payment_time,
         finish_date_id,
         finish_time,
         order_original_amount,
         order_activity_amount,
         order_coupon_amount,
         order_total_amount,
         payment_amount
     from dwd_trade_trade_flow_acc
     where dt = '9999-12-31'
     union all
     select
         data.id                                     as order_id,
         data.user_id,
         data.province_id,
         date_format(data.create_time, 'yyyy-MM-dd') as order_date_id,
         data.create_time                            as order_time,
         null                                        as payment_date_id,
         null                                        as payment_time,
         null                                        as finish_date_id,
         null                                        as finish_time,
         data.original_total_amount                  as order_original_amount,
         data.activity_reduce_amount                 as order_activity_amount,
         data.coupon_reduce_amount                   as order_coupon_amount,
         data.total_amount                           as order_total_amount,
         null                                        as payment_amount
     from ods_order_info_inc
     where dt = '2022-06-09'
       and type = 'insert'
     ) oii
left join (
          select
              data.order_id,
              data.callback_time       as payment_time,
              date(data.callback_time) as payment_date_id,
              data.total_amount        as payment_amount
          from ods_payment_info_inc
          where dt = '2022-06-09'
            and type = 'update'
            and array_contains(map_keys(old), 'payment_status')
            and data.payment_status = '1602'
          ) pii
  on oii.order_id = pii.order_id
left join(
         select
             data.order_id,
             data.create_time       as finish_time,
             date(data.create_time) as finish_date_id
         from ods_order_status_log_inc
         where dt = '2022-06-09'
           and type = 'insert'
           and data.order_status = '1004'
         ) osl
  on oii.order_id = osl.order_id;

-- 6工具域优惠券使用(支付)事务事实表
--     时间 用户 优惠券 订单 金额
DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_tool_coupon_used_inc partition (dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date(data.used_time),
    data.used_time,
    date(data.used_time)
from ods_coupon_use_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert'
  and data.used_time is not null;
-- 每日
insert overwrite table dwd_tool_coupon_used_inc partition (dt = '2022-06-09')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date(data.used_time),
    data.used_time
from ods_coupon_use_inc
where dt = '2022-06-09'
  and type = 'update'
  and array_contains(map_keys(old), 'used_time');

-- 7互动域收藏商品事务事实表
--     时间 用户 商品
DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date(data.create_time) as date_id,
    data.create_time,
    date(data.create_time) as date_id
from ods_favor_info_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert';
-- 每日
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2022-06-09')
select
    data.id,
    data.user_id,
    data.sku_id,
    date(data.create_time) as date_id,
    data.create_time
from ods_favor_info_inc
where dt = '2022-06-09'
  and type = 'insert';

-- 8流量域页面浏览事务事实表
--     时间 用户(访客) 页面 停留时间 上一个页面
--     页面浏览日志什么时候产生:
--         浏览终端产生日志 -> action -> out -> 服务终端(server) -> log
--         日志表中存在两类数据: App启动，页面浏览
--     Hive Bug: struct判空失效
--         解决方案:
--             1. 增加配置，禁止cbo优化
--             2. 对struct中属性进行判空
--     会话: 数据的通信状态
--         连续的页面访问和会话相关
--         登录功能也和会话相关，如果会话发生变化，那么需要重新登陆
DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`      STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页ID',
    `page_id`        STRING COMMENT '页面ID ',
    `from_pos_id`    STRING COMMENT '点击坑位ID',
    `from_pos_seq`   STRING COMMENT '点击坑位位置',
    `refer_id`       STRING COMMENT '营销渠道ID',
    `date_id`        STRING COMMENT '日期ID',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话ID',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2022-06-08')
select
    common.ar                                                           as province_id,
    common.ba                                                           as brand,
    common.ch                                                           as channel,
    common.is_new                                                       as is_new,
    common.md                                                           as model,
    common.mid                                                          as mid_id,
    common.os                                                           as operate_system,
    common.uid                                                          as user_id,
    common.vc                                                           as version_code,
    page.item                                                           as page_item,
    page.item_type                                                      as page_item_type,
    page.last_page_id                                                   as last_page_id,
    page.page_id                                                        as page_id,
    page.from_pos_id                                                    as from_pos_id,
    page.from_pos_seq                                                   as from_pos_seq,
    page.refer_id                                                       as refer_id,
    date_format(from_utc_timestamp(ts, "GMT+8"), 'yyyy-MM-dd')          as date_id,
    date_format(from_utc_timestamp(ts, "GMT+8"), 'yyyy-MM-dd hh:mm:ss') as view_time,
    common.sid                                                          as session_id,
    page.during_time                                                    as during_time
from ods_log_inc
where dt = '2022-06-08'
  and page.page_id is not null;

-- 9用户域用户注册事务事实表
--     时间 用户
DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 数据装载 从ods_log_inc取数据
insert overwrite table dwd_user_register_inc partition (dt = '2022-06-08')
select
    common.uid                                                                                     as user_id,
    date_format(from_utc_timestamp(ts + bigint(page.during_time), "GMT+8"), 'yyyy-MM-dd')          as date_id,
    date_format(from_utc_timestamp(ts + bigint(page.during_time), "GMT+8"), 'yyyy-MM-dd hh:mm:ss') as create_time,
    common.ch                                                                                      as channel,
    common.ar                                                                                      as province_id,
    common.vc                                                                                      as version_code,
    common.mid                                                                                     as mid_id,
    common.ba                                                                                      as brand,
    common.md                                                                                      as model,
    common.os                                                                                      as operate_system
from ods_log_inc
where dt = '2022-06-08'
  and page.page_id = 'register'
  and common.uid is not null;
-- 首日 从ods_user_info_inc取数据
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition (dt)
select
    user_info.user_id,
    date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_id
from (
     select
         data.id                                     as user_id,
         date_format(data.create_time, 'yyyy-MM-dd') as date_id,
         data.create_time                            as create_time
     from ods_user_info_inc
     where dt = '2022-06-08'
       and type = 'bootstrap-insert'
     ) as user_info
left join
(
select
    common.uid as user_id,
    common.ch  as channel,
    common.ar  as province_id,
    common.vc  as version_code,
    common.mid as mid_id,
    common.ba  as brand,
    common.md  as model,
    common.os  as operate_system
from ods_log_inc
where dt = '2022-06-08'
  and page.page_id = 'register'
) as log
  on user_info.user_id = log.user_id;
-- 每日
insert overwrite table dwd_user_register_inc partition (dt = '2022-06-09')
select
    user_info.user_id,
    date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
     select
         data.id                                     as user_id,
         date_format(data.create_time, 'yyyy-MM-dd') as date_id,
         data.create_time                            as create_time
     from ods_user_info_inc
     where dt = '2022-06-09'
       and type = 'insert'
     ) as user_info
left join
(
select
    common.uid as user_id,
    common.ch  as channel,
    common.ar  as province_id,
    common.vc  as version_code,
    common.mid as mid_id,
    common.ba  as brand,
    common.md  as model,
    common.os  as operate_system
from ods_log_inc
where dt = '2022-06-09'
  and page.page_id = 'register'
) as log
  on user_info.user_id = log.user_id;

-- 10用户域用户登录事务事实表
--     时间 用户
--     实际生产环境中登录不一定有登陆页面
DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 首日
insert overwrite table dwd_user_login_inc partition (dt = '2022-06-08')
select
    user_id,
    date_format(from_utc_timestamp(ts, "GMT+8"), 'yyyy-MM-dd')          as date_id,
    date_format(from_utc_timestamp(ts, "GMT+8"), 'yyyy-MM-dd hh:mm:ss') as login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
     select
         row_number() over (partition by common.sid order by ts) as rk,
         common.uid                                              as user_id,
         common.ch                                               as channel,
         common.ar                                               as province_id,
         common.vc                                               as version_code,
         common.mid                                              as mid_id,
         common.ba                                               as brand,
         common.md                                               as model,
         common.os                                               as operate_system,
         ts,
         page.during_time
     from ods_log_inc
     where dt = '2022-06-08'
       and common.uid is not null
       and page.page_id is not null
     ) log
where rk = 1;