-- 1商品维度表
DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING COMMENT 'SKU_ID',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    `spu_id`               STRING COMMENT 'SPU编号',
    `spu_name`             STRING COMMENT 'SPU名称',
    `category3_id`         STRING COMMENT '三级品类ID',
    `category3_name`       STRING COMMENT '三级品类名称',
    `category2_id`         STRING COMMENT '二级品类id',
    `category2_name`       STRING COMMENT '二级品类名称',
    `category1_id`         STRING COMMENT '一级品类ID',
    `category1_name`       STRING COMMENT '一级品类名称',
    `tm_id`                STRING COMMENT '品牌ID',
    `tm_name`              STRING COMMENT '品牌名称',
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING, value_id :STRING, attr_name :STRING, value_name
                                        :STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING, sale_attr_value_id :STRING, sale_attr_name :STRING,
                                        sale_attr_value_name :STRING>> COMMENT '销售属性',
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据载入
insert overwrite table dim_sku_full partition (dt = '2022-06-08')
select
    id,
    price,
    sku_name,
    sku_desc,
    weight,
    is_sale,
    sku_info.spu_id,
    spu_name,
    category3.category3_id,
    category3_name,
    category2.category2_id,
    category2_name,
    category1.category1_id,
    category1_name,
    sku_info.tm_id,
    tm_name,
    sku_attr_values,
    sku_sale_attr_values,
    create_time
from (
     select
         id,
         spu_id,
         price,
         sku_name,
         sku_desc,
         weight,
         tm_id,
         category3_id,
         is_sale,
         create_time
     from ods_sku_info_full
     where dt = '2022-06-08'
     ) as sku_info
left join (
          select id as spu_id, spu_name from ods_spu_info_full where dt = '2022-06-08'
          ) as spu_info
  on sku_info.spu_id = spu_info.spu_id
left join (
          select
              id   as category3_id,
              name as category3_name,
              category2_id
          from ods_base_category3_full
          where dt = '2022-06-08'
          ) as category3
  on sku_info.category3_id = category3.category3_id
left join (
          select
              id   as category2_id,
              name as category2_name,
              category1_id
          from ods_base_category2_full
          where dt = '2022-06-08'
          ) as category2
  on category3.category2_id = category2.category2_id
left join (
          select
              id   as category1_id,
              name as category1_name
          from ods_base_category1_full
          where dt = '2022-06-08'
          ) as category1
  on category2.category1_id = category1.category1_id
left join (
          select
              id as tm_id,
              tm_name
          from ods_base_trademark_full
          where dt = '2022-06-08'
          ) as trademark
  on sku_info.tm_id = trademark.tm_id
left join (
          select
              sku_id,
              collect_list(struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name,
                                  'value_name', value_name)) as sku_attr_values
          from ods_sku_attr_value_full
          where dt = '2022-06-08'
          group by sku_id
          ) as attr_value
  on sku_info.id = attr_value.sku_id
left join (
          select
              sku_id,
              collect_list(struct('sale_attr_id', sale_attr_id, 'sale_value_id', sale_attr_value_id,
                                  'sale_attr_name', sale_attr_name,
                                  'sale_value_name', sale_attr_value_name)) as sku_sale_attr_values
          from ods_sku_sale_attr_value_full
          where dt = '2022-06-08'
          group by sku_id
          ) as sale_attr_value
  on sku_info.id = sale_attr_value.sku_id;

-- 2优惠券维度表
DROP TABLE IF EXISTS dim_coupon_full;
CREATE EXTERNAL TABLE dim_coupon_full
(
    `id`               STRING COMMENT '优惠券编号',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减免金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       STRING COMMENT '可以领取的开始时间',
    `end_time`         STRING COMMENT '可以领取的结束时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据载入
insert overwrite table dim_coupon_full partition (dt = '2022-06-08')
select
    id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type_code
        when '3201' then concat('满', condition_amount, '减', benefit_amount)
        when '3202' then concat('满', condition_num, '件打', benefit_discount, '折')
        when '3203' then concat('减', benefit_amount)
        end
        as benefit_rule,
    create_time,
    range_type_code,
    range_type_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from (
     select
         id,
         coupon_name,
         coupon_type as coupon_type_code,
         condition_amount,
         condition_num,
         activity_id,
         benefit_amount,
         benefit_discount,
         create_time,
         range_type  as range_type_code,
         limit_num,
         taken_count,
         start_time,
         end_time,
         operate_time,
         expire_time
     from ods_coupon_info_full
     where dt = '2022-06-08'
     ) as coupon_info
left join (
          select
              dic_code,
              dic_name as coupon_type_name
          from ods_base_dic_full
          where dt = '2022-06-08'
            and parent_code = '32'
          ) as dic1
  on coupon_info.coupon_type_code = dic1.dic_code
left join (
          select
              dic_code,
              dic_name as range_type_name
          from ods_base_dic_full
          where dt = '2022-06-08'
            and parent_code = '33'
          ) as dic2
  on coupon_info.range_type_code = dic1.dic_code;

-- 3.活动维度表
DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`        STRING COMMENT '活动ID',
    `activity_name`      STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`      STRING COMMENT '活动描述',
    `start_time`         STRING COMMENT '开始时间',
    `end_time`           STRING COMMENT '结束时间',
    `create_time`        STRING COMMENT '创建时间',
    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`      BIGINT COMMENT '满减件数',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`       STRING COMMENT '优惠规则',
    `benefit_level`      STRING COMMENT '优惠级别'
) COMMENT '活动维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据载入
insert overwrite table dim_activity_full partition (dt = '2022-06-08')
select
    activity_rule_id,
    activity_rule.activity_id,
    activity_name,
    activity_type_code,
    activity_type_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case activity_type_code
        when '3101' then concat('满', condition_amount, '减', benefit_amount)
        when '3102' then concat('满', condition_num, '件打', benefit_discount, '折')
        when '3103' then concat('减', benefit_amount)
        end
        as benefit_rule,
    benefit_level
from (
     select
         id            as activity_rule_id,
         activity_id,
         activity_type as activity_type_code,
         condition_amount,
         condition_num,
         benefit_amount,
         benefit_discount,
         benefit_level,
         create_time
     from ods_activity_rule_full
     where dt = '2022-06-08'
     ) as activity_rule
left join (
          select
              id as activity_id,
              activity_name,
              activity_desc,
              start_time,
              end_time
          from ods_activity_info_full
          where dt = '2022-06-08'
          ) as activity_info
  on activity_rule.activity_id = activity_info.activity_id
left join (
          select
              dic_code,
              dic_name as activity_type_name
          from ods_base_dic_full
          where dt = '2022-06-08'
            and parent_code = '31'
          ) dic
  on activity_rule.activity_type_code = dic.dic_code;

-- 8用户维度表
DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表' PARTITIONED BY (`dt` STRING)
    STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_user_zip/' TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select
    data.id           as id,
    data.name         as name,
    data.phone_num    as phone_num,
    data.email        as email,
    data.user_level   as user_level,
    data.birthday     as birthday,
    data.gender       as gender,
    data.create_time  as create_time,
    data.operate_time as operate_time,
    '2022-06-08',
    '9999-12-31'
from ods_user_info_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert';
-- 每日
set hive.exec.dynamic.partition.mode=nonstrict;
with today as (
              select
                  id,
                  name,
                  phone_num,
                  email,
                  user_level,
                  birthday,
                  gender,
                  create_time,
                  operate_time,
                  start_date,
                  end_date
              from (
                   select
                       data.id                                             as id,
                       data.name                                           as name,
                       data.phone_num                                      as phone_num,
                       data.email                                          as email,
                       data.user_level                                     as user_level,
                       data.birthday                                       as birthday,
                       data.gender                                         as gender,
                       data.create_time                                    as create_time,
                       data.operate_time                                   as operate_time,
                       '2022-06-09'                                        as start_date,
                       '9999-12-31'                                        as end_date,
                       rank() over (partition by data.id order by ts desc) as rk
                   from ods_user_info_inc
                   where dt = '2022-06-09'
                     and type in ('insert', 'update')
                   ) t1
              where rk = 1
              )
insert
overwrite
table
dim_user_zip
partition
(
dt
)
select
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    if(rk = 2, date_sub('2022-06-09', 1), '9999-12-31') as end_date,
    if(rk = 2, date_sub('2022-06-09', 1), '9999-12-31') as dt
from (
     select *,
            rank() over (partition by id order by start_date desc) as rk
     from (
          select
              id,
              name,
              phone_num,
              email,
              user_level,
              birthday,
              gender,
              create_time,
              operate_time,
              start_date,
              end_date
          from dim_user_zip
          where dt = '9999-12-31'
          union
          select *
          from today
          ) t2
     ) t3;