-- Active: 1727143613894@@127.0.0.1@3306@db1
drop table if exists t1;

create table if not exists t1
(
    id   bigint,
    word string
);

insert into t1
values (6, "大卷手纸");

drop function ik_word_split;

create function ik_word_split
    as "com.atguigu.hive.SplitWord" using jar "hdfs://hadoop102:8020/tmp/HiveFunction-1.jar";

select *
from t1;

select id,
       keyword
from t1 lateral view ik_word_split(word) lv as keyword;

drop table if exists test010101;

create table if not exists test010101
(
    id       bigint,
    add_time varchar(20),
    state    bigint
);

insert into test010101
values (1, "2022-01-01", 1),
       (1, "2022-01-02", 2),
       (1, "2022-01-03", 3),
       (2, "2022-01-01", 1),
       (2, "2022-01-02", 1),
       (3, "2022-01-03", 2),
       (3, "2022-01-04", 1),
       (3, "2022-01-05", 2),
       (3, "2022-01-06", 4);

select t1.id,
       start_time,
       t2.state,
       end_time,
       t3.state
from (select id,
             min(add_time) as start_time,
             max(add_time) as end_time
      from test010101
      group by id) as t1
left join test010101 as t2
  on t1.id = t2.id
    and t1.start_time = t2.add_time
left join test010101 as t3
  on t1.id = t3.id
    and t1.end_time = t3.add_time;