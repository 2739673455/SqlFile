drop      table if exists edu.dws_test_course_paper_score_duration_window;

create    table if not exists edu.dws_test_course_paper_score_duration_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `course_id` bigint comment '课程id',
          `course_name` varchar(200) comment '课程名称',
          `paper_id` bigint comment '试卷id',
          `paper_title` varchar(200) comment '试卷名称',
          `view_count` bigint replace comment '考试次数',
          `course_user_count` bigint replace comment '课程粒度考试人数',
          `paper_user_count` bigint replace comment '试卷粒度考试人数',
          `score` decimal(16, 2) replace comment '总分',
          `duration_sec` bigint replace comment '总时长'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `course_id`, `course_name`, `paper_id`, `paper_title`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

create    table if not exists edu.dws_traffic_keyword_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `keyword` varchar(200) comment '关键词',
          `keyword_count` bigint replace comment '关键词计数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `keyword`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

drop      table if exists edu.dws_test_question_correct_uu_window;

create    table if not exists edu.dws_test_question_correct_uu_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `question_id` bigint comment '题目id',
          `question_txt` varchar(2000) comment '题目内容',
          `count` bigint replace comment '答题次数',
          `correct_count` bigint replace comment '正确答题次数',
          `uu_count` bigint replace comment '答题独立用户数',
          `correct_uu_count` bigint replace comment '正确答题独立用户数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `question_id`, `question_txt`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

create    table if not exists edu.dws_test_paper_score_distribution_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `paper_id` bigint comment '试卷id',
          `paper_title` varchar(200) comment '试卷名称',
          `score_range` varchar(200) comment '分数段',
          `uv` bigint replace comment '人数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `paper_id`, `paper_title`, `score_range`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

create    table if not exists edu.dws_interaction_review_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `course_id` bigint comment '课程id',
          `course_name` varchar(200) comment '课程名称',
          `uv` bigint replace comment '评价人数',
          `review_stars` bigint replace comment '评价总分',
          `star5` bigint replace comment '好评数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `course_id`, `course_name`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

create    table if not exists edu.dws_trade_total_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `final_amount` decimal(16, 2) replace comment '下单总额',
          `uv` bigint replace comment '下单人数',
          `vv` bigint replace comment '下单次数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

drop      table if exists edu.dws_user_funnel_window;

create    table if not exists edu.dws_user_funnel_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `home_count` bigint replace comment '首页浏览人数',
          `course_detail_count` bigint replace comment '商品详情页浏览人数',
          `cart_add_count` bigint replace comment '加购人数',
          `order_count` bigint replace comment '下单人数',
          `pay_success_count` bigint replace comment '支付成功人数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );

drop      table if exists edu.dws_trade_province_order_window;

create    table if not exists edu.dws_trade_province_order_window (
          `start_time` datetime comment '窗口起始时间',
          `end_time` datetime comment '窗口结束时间',
          `current_date` date comment '当天日期',
          `province_id` bigint comment '省份id',
          `province_name` varchar(200) comment '省份名称',
          `final_amount` decimal(16, 2) replace comment '下单金额',
          `order_count` bigint replace comment '下单次数',
          `uu_order_count` bigint replace comment '下单人数'
          ) engine = olap aggregate key (`start_time`, `end_time`, `current_date`, `province_id`, `province_name`)
partition by range (`current_date`) () distributed by hash(`start_time`) buckets 10 properties (
          "replication_num" = "2",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "day",
          "dynamic_partition.end" = "3",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10"
          );