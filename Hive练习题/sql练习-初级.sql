set hive.exec.mode.local.auto = true;
-- 2.1.1
select * from student_info where stu_name rlike '.*冰.*';
-- 2.1.2
select count(*) from teacher_info where tea_name rlike '王.*';
-- 2.1.3
select stu_id, score, course_id
from score_info
where
    course_id = '04'
    and score < 60
order by score desc;
-- 2.1.4
select t1.stu_id, student_info.stu_name, t1.score
from (
        select *
        from score_info
        where
            score < 60
            and course_id = (
                select course_id
                from course_info
                where
                    course_name = '数学'
            )
    ) t1
    join student_info on t1.stu_id = student_info.stu_id;
-- 3.1.1
select sum(score) from score_info where course_id = '02';
-- 3.1.2
select count(distinct stu_id) from score_info;
-- 3.2.1
select course_id, max(score) `最高分`, min(score) `最低分`
from score_info
group by
    course_id;
-- 3.2.2
select course_id, count(stu_id) from score_info group by course_id;
-- 3.2.3
select sex, count(*) from student_info group by sex;
-- 3.3.1
select student_info.stu_id, t1.avgscore
from student_info
    right join (
        select stu_id, avg(score) avgscore
        from score_info
        group by
            stu_id
        having
            avg(score) > 60
    ) t1 on student_info.stu_id = t1.stu_id;
-- 3.3.2
select stu_id, count(course_id) num
from score_info
group by
    stu_id
having
    num > 3;
-- 3.3.4
select avg(score) avgscore, course_id
from score_info
group by
    course_id
order by avgscore, course_id desc;
-- 3.3.5
select course_id, count(*)
from score_info
group by
    course_id
having
    count(*) > 14;
-- 3.4.1
select stu_id, sum(score)
from score_info
group by
    stu_id
order by sum(score) desc;
-- 3.4.3
select stu_id, student_info.stu_name
from student_info
where
    student_info.stu_id in (
        select t1.stu_id stuid
        from (
                select stu_id
                from score_info
                group by
                    stu_id
                having
                    count(*) = 3
            ) t1
            join (
                select stu_id
                from score_info
                where
                    course_id = (
                        select course_id
                        from course_info
                        where
                            course_name = '语文'
                    )
            ) t2 on t1.stu_id = t2.stu_id
    );
-- 4.1.2
select student_info.stu_id, student_info.stu_name
from student_info
    left join score_info on student_info.stu_id = score_info.stu_id
group by
    student_info.stu_id,
    student_info.stu_name
having
    count(*) < (
        select count(*)
        from course_info
    );
-- 4.1.3
select student_info.stu_id, student_info.stu_name
from student_info
    right join (
        select stu_id
        from score_info
        group by
            stu_id
        having
            count(course_id) = 3
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.1.2
select student_info.stu_id, student_info.stu_name, nvl(t1.course_count, 0), nvl(t1.sum_score, 0)
from student_info
    left join (
        select
            stu_id, count(course_id) course_count, sum(score) sum_score
        from score_info
        group by
            stu_id
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.1.3
select t1.stu_id, student_info.stu_name, t1.avg_score
from student_info
    right join (
        select stu_id, avg(score) avg_score
        from score_info
        group by
            stu_id
        having
            avg(score) > 85
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.1.4
select student_info.stu_id, student_info.stu_name, t1.course_id, t1.course_name
from student_info
    right join (
        select score_info.stu_id, course_info.course_id, course_info.course_name
        from score_info
            left join course_info on score_info.course_id = course_info.course_id
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.1.6
select student_info.stu_id, student_info.stu_name, t1.course_id, t1.course_name
from student_info
    right join (
        select score_info.stu_id, score_info.course_id, course_info.course_name
        from score_info
            join course_info on score_info.course_id = course_info.course_id
        where
            course_info.course_id = '03'
            and score > 80
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.2.1
select student_info.stu_id, student_info.stu_name, t1.score
from student_info
    right join (
        select stu_id, score
        from score_info
        where
            course_id = '01'
            and score < 60
    ) t1 on student_info.stu_id = t1.stu_id
order by score desc;
-- 5.2.2
select student_info.stu_id, student_info.stu_name, t2.course_name, t2.score
from student_info
    right join (
        select score_info.stu_id, course_info.course_name, score_info.score
        from
            score_info
            join (
                select stu_id
                from score_info
                group by
                    stu_id
                having
                    min(score) > 70
            ) t1 on score_info.stu_id = t1.stu_id
            join course_info on score_info.course_id = course_info.course_id
    ) t2 on student_info.stu_id = t2.stu_id;
-- 5.2.3
select s1.stu_id, s1.course_id, s1.score
from
    score_info s1
    join score_info s2 on s1.score = s2.score
    and s1.stu_id = s2.stu_id
    and s1.course_id <> s2.course_id;
-- 5.2.4
select s1.stu_id, s1.score, s2.score
from
    score_info s1
    join score_info s2 on s1.stu_id = s2.stu_id
    and s1.course_id = '01'
    and s2.course_id = '02'
where
    s1.score > s2.score;
-- 5.2.5
select student_info.stu_id, student_info.stu_name
from student_info
    right join (
        select s1.stu_id
        from
            score_info s1
            join score_info s2 on s1.stu_id = s2.stu_id
            and s1.course_id = '01'
            and s2.course_id = '02'
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.2.6
select student_info.stu_id, student_info.stu_name
from student_info
    right join (
        select stu_id
        from score_info
        where
            score_info.course_id in (
                select course_id
                from course_info
                where
                    course_info.tea_id = (
                        select tea_id
                        from teacher_info
                        where
                            tea_name = '李体音'
                    )
            )
        group by
            stu_id
        having
            count(course_id) = (
                select count(course_id)
                from course_info
                where
                    course_info.tea_id = (
                        select tea_id
                        from teacher_info
                        where
                            tea_name = '李体音'
                    )
            )
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.2.7
select student_info.stu_id, student_info.stu_name
from student_info
    right join (
        select distinct
            stu_id
        from score_info
        where
            score_info.course_id in (
                select course_id
                from course_info
                where
                    course_info.tea_id = (
                        select tea_id
                        from teacher_info
                        where
                            tea_name = '李体音'
                    )
            )
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.2.8
select student_info.stu_id, student_info.stu_name
from student_info
where
    student_info.stu_id not in (
        select distinct
            score_info.stu_id
        from score_info
        where
            score_info.course_id in (
                select course_id
                from course_info
                where
                    tea_id = (
                        select tea_id
                        from teacher_info
                        where
                            tea_name = '李体音'
                    )
            )
    );
-- 5.2.9
select student_info.stu_id, student_info.stu_name
from student_info
    right join (
        select distinct
            score_info.stu_id
        from score_info
        where
            score_info.course_id in (
                select course_id
                from score_info
                where
                    stu_id = '001'
            )
            and score_info.stu_id <> '001'
    ) t1 on student_info.stu_id = t1.stu_id;
-- 5.2.10
select student_info.stu_name, course_info.course_name, score_info.score, t1.avg_score
from
    student_info
    join score_info on student_info.stu_id = score_info.stu_id
    join (
        select stu_id, avg(score) avg_score
        from score_info
        group by
            stu_id
    ) t1 on student_info.stu_id = t1.stu_id
    join course_info on course_info.course_id = score_info.course_id
order by t1.avg_score desc;