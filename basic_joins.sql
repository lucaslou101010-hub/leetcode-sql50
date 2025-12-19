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

