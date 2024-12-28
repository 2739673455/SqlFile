-- 1.1.4 关键词统计
select    keyword,
          sum(keyword_count) as keywordCount
from      edu.dws_traffic_keyword_window partition par20241115
group by  keyword;

-- 1.2.3 用户行为漏斗分析
select    sum(home_count)          as homeCount,
          sum(course_detail_count) as courseDetailCount,
          sum(cart_add_count)      as cartAddCount,
          sum(order_count)         as orderCount,
          sum(pay_success_count)   as paySuccessCount
from      edu.dws_user_funnel_window partition par20241116;

-- 1.3.2 评价情况
select    course_id,
          course_name,
          cast(sum(review_stars) / sum(uv) as decimal(16, 2)) as avgStars,
          sum(uv)                                             as uv,
          cast(sum(star5) / sum(uv) as decimal(16, 2))        as star5Rate
from      edu.dws_interaction_review_window partition par20241116
group by  course_id,
          course_name;

-- 1.4.2 各省份交易统计
select    province_name       as provinceName,
          sum(final_amount)   as finalAmount,
          sum(uu_order_count) as uv,
          sum(order_count)    as vv
from      edu.dws_trade_province_order_window partition par20241118
group by  province_name;

-- 1.4.3 交易综合统计
select    `current_date`    as currentDate,
          sum(final_amount) as finalAmount,
          sum(uv)           as uv,
          sum(vv)           as vv
from      edu.dws_trade_total_window
where     `current_date` between date_sub('2024-11-17', 3) and '2024-11-17'
group by  `current_date`;

-- 1.5.1 各试卷考试统计
select    paper_id                                            as paperId,
          paper_title                                         as paperTitle,
          sum(paper_user_count)                               as uv,
          sum(score) / sum(view_count)                        as avgScore,
          cast(sum(duration_sec) / sum(view_count) as bigint) as avgDurationSec
from      edu.dws_test_course_paper_score_duration_window partition par20241116
group by  paper_id,
          paper_title;

-- 1.5.2 各课程考试统计
select    course_id                                           as courseId,
          course_name                                         as courseName,
          sum(course_user_count)                              as uv,
          sum(score) / sum(view_count)                        as avgScore,
          cast(sum(duration_sec) / sum(view_count) as bigint) as avgDurationSec
from      edu.dws_test_course_paper_score_duration_window partition par20241116
group by  course_id,
          course_name;

-- 1.5.3 各试卷成绩分布
select    paper_id    as paperId,
          paper_title as paperTitle,
          score_range as scoreRange,
          sum(uv)     as uv
from      edu.dws_test_paper_score_distribution_window partition par20241115
group by  paper_id,
          paper_title,
          score_range;

-- 1.5.4 答题情况统计
select    question_id                           as questionId,
          question_txt                          as questionTxt,
          sum(correct_count)                    as correctCount,
          sum(`count`)                          as `count`,
          sum(correct_count) / sum(`count`)     as correctRate,
          sum(correct_uu_count)                 as correctUuCount,
          sum(uu_count)                         as uuCount,
          sum(correct_uu_count) / sum(uu_count) as correctUuRate
from      edu.dws_test_question_correct_uu_window partition par20241116
group by  question_id,
          question_txt;