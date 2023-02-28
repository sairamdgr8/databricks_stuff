--- create min and maximun sequencing set of sequencing rows
------  https://mattboegner.com/improve-your-sql-skills-master-the-gaps-islands-problem/
----  https://www.linkedin.com/posts/sairam-p-l_sql-interviewprep-interview-activity-6899332334727106560-ep4n

--- 

CREATE TABLE seq_gen (
    ID varchar(2),
    seq  int
	);
INSERT ALL 
     into  seq_gen (ID, seq) VALUES ('A',1)
     into  seq_gen (ID, seq) VALUES ('A',2)
     into  seq_gen (ID, seq) VALUES ('A',3)
     into  seq_gen (ID, seq) VALUES ('A',5)
     into  seq_gen (ID, seq) VALUES ('A',6)
     into  seq_gen (ID, seq) VALUES ('A',9)
     into  seq_gen (ID, seq) VALUES ('A',10)
     into  seq_gen (ID, seq) VALUES ('A',15)
     into  seq_gen (ID, seq) VALUES ('B',17)
     into  seq_gen (ID, seq) VALUES ('B',18)
     into  seq_gen (ID, seq) VALUES ('B',21)
     into  seq_gen (ID, seq) VALUES ('C',25)
   SELECT 1 FROM DUAL;
   
   
SELECT * FROM seq_gen;
   
--- drop    table seq_gen
   
select id,seq,seq-row_number() over(  order by seq  ) as seq_1 from seq_gen


with mytab as(   
select id,seq,seq-row_number() over(  order by seq  ) as seq_1 from seq_gen
)
select seq_1,string_agg(seq_1,',') seq_grp
from mytab
group by seq_1
order by seq_1



------------ half working

with mytab as(   
select id,seq,seq-row_number() over(  order by seq  ) as seq_1 from seq_gen
)
select seq_1,listagg(seq,',') seq_grp
from mytab
group by seq_1




-------- another way  -- working

select id,
      case when min(seq)=max(seq)
       then Null
       else
       min(seq) end as min_seq,
       max(seq) as max_seq
from       
      
(select id,seq,seq-row_number() over(  order by seq  ) as seq_1 from seq_gen)
group by id,seq_1
order by 1

-------------------------------------------------------------------------------------------
with mytab as(   
select id,seq,seq-row_number() over(  order by seq  ) as seq_1 from seq_gen
)select * from mytab



;with grouped_data as (
 select
  ROW_NUMBER() over (order by id) as rn,
  numbers,
  numbers - (ROW_NUMBER() over (order by id)) as groupid
 from [data islands]
), data_island as (
 select
  rn,
  numbers,
  groupid,
  count(*) over (partition by groupid) as item_count
 from grouped_data
)
select
 ROW_NUMBER() over (order by groupid) as data_island_no,
 string_agg(numbers,',') data_island,
 min(numbers) as Min_Boundary,
 max(numbers) as Max_Boundary,
 max(item_count) as Item_Count
from data_island
group by groupid
order by groupid
   
   

 

