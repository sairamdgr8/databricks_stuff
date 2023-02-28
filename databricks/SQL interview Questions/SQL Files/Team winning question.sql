CREATE table scorecard  (  
    t1 varchar(255),  
    t2 varchar(255),  
    res varchar(255)  
);


INSERT INTO scorecard (t1, t2, res) 
VALUES ('A', 'B', 'A');

INSERT INTO scorecard (t1, t2, res) 
VALUES ('A', 'C', 'C');

INSERT INTO scorecard (t1, t2, res) 
VALUES ('B', 'D', 'D');

INSERT INTO scorecard (t1, t2, res) 
VALUES ('A', 'D', 'A');

select distinct t1,t2 as tf from t ;

--simple union all will do the trick: 
select t, sum(is_win) as won, sum(is_loss) as loss, sum(is_tie) as tie
from ((select t1 as t,
(case when res = t1 then 1 else 0 end) as is_win,
(case when res = t2 then 1 else 0 end) as is_loss,
(case when res is null then 1 else 0 end) as is_tie
from scorecard
) union all
(select t2 as t,
(case when res = t2 then 1 else 0 end) as is_win,
(case when res = t1 then 1 else 0 end) as is_loss,
(case when res is null then 1 else 0 end) as is_tie
from scorecard
)
) t
group by t
order by t;


 create table basic1 as select t1 as t,
(case when res = t1 then 1 else 0 end) as is_win,
(case when res = t2 then 1 else 0 end) as is_loss,
(case when res is null then 1 else 0 end) as is_tie
from scorecard


create table basic2 as select t1 as t,
(case when res = t1 then 1 else 0 end) as is_win,
(case when res = t2 then 1 else 0 end) as is_loss,
(case when res is null then 1 else 0 end) as is_tie
from scorecard


select * from basic1
union all
select * from basic2