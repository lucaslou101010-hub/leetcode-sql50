# Write your MySQL query statement below
select w1.id
from Weather w1
left join Weather w2 on datediff(w1.recordDate,w2.recordDate) = 1
where w1.temperature > w2.temperature;

# Write your MySQL query statement below
select 
    machine_id,
    ROUND(AVG(processing_time),3) as processing_time
from (
    select
        a1.machine_id,
        a2.timestamp-a1.timestamp as processing_time
    from Activity as a1
    left join Activity as a2
    on 
        a1.machine_id = a2.machine_id
        and a1.process_id = a2.process_id
        and a1.activity_type = 'start'
        and a2.activity_type = 'end'
) as p
group by machine_id;

# Write your MySQL query statement below
select
    name,
    bonus
from (
    select
        e.name,
        b.bonus
    from Employee as e
    left join Bonus as b
    on e.empId = b.empId
) as eb
where eb.bonus < 1000 or eb.bonus is null;

# Note 'join' is actually inner join in SQL!

# Write your MySQL query statement below
select
    s.student_id,
    s.student_name,
    sub.subject_name,
    COUNT(e.subject_name) as attended_exams
from 
    Students as s
cross join
    Subjects as sub
left join Examinations as e
on s.student_id = e.student_id and sub.subject_name = e.subject_name
group by s.student_id,s.student_name,sub.subject_name
order by s.student_id,sub.subject_name;

# Write your MySQL query statement below
select
    e.name
from (
    select
        e2.managerId as id,
        COUNT(e2.id) as direct_reports
    from Employee as e2
    group by e2.managerId
) as dr
join Employee as e
on e.id = dr.id
where dr.direct_reports >= 5;

SELECT name 
FROM Employee 
WHERE id IN (
    SELECT managerId 
    FROM Employee 
    GROUP BY managerId 
    HAVING COUNT(id) >= 5
);

# Write your MySQL query statement below
select 
    s.user_id,
    round(ifnull(cc.confirmation_rate,0.),2) as confirmation_rate
from Signups s
left join (
    select
        c.user_id as user_id,
        avg(case when c.action = 'confirmed' then 1 else 0 end) as confirmation_rate
    from Confirmations c
    group by c.user_id
) as cc 
on cc.user_id = s.user_id;
