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

# Write your MySQL query statement below
select 
    round(sum(d2.is_immediate) / count(d2.is_immediate) * 100,2) as immediate_percentage
from (
    select
        case when d1.order_date = d1.customer_pref_delivery_date then 1 else 0 end as is_immediate,
        row_number() OVER (PARTITION BY d1.customer_id ORDER BY d1.order_date ASC) as rn
    from Delivery as d1
) as d2
where d2.rn = 1;

# Write your MySQL query statement below
select
    s2.product_id,
    s2.year as first_year,
    sum(s2.quantity) as quantity,
    s2.price
from (
    select
        s.product_id,
        dense_rank() over (partition by s.product_id order by s.year asc) as rn,
        s.year,
        s.quantity,
        s.price
    from Sales as s
) as s2
where s2.rn = 1;
group by s2.product_id,s2.year,s2.price;

# row_number, rank, dense_rank
    
SELECT 
    product_id, 
    year AS first_year, 
    quantity, 
    price
FROM Sales
WHERE (product_id, year) IN (
    SELECT product_id, MIN(year)
    FROM Sales
    GROUP BY product_id
);

# Write your MySQL query statement below
select
    c2.customer_id
from (
    select
        c.customer_id,
        count(distinct c.product_key) as num_bought_product
    from Customer as c
    group by c.customer_id
) as c2
where c2.num_bought_product = (select count(*) from Product);
