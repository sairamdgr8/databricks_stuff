Create table elevator_max_person(
elev_name varchar(10),
elev_order int,
elev_weight int
);

INSERT ALL   
  INTO elevator_max_person (elev_name,elev_order,elev_weight) VALUES ('a',1,70)   
  INTO elevator_max_person (elev_name,elev_order,elev_weight) VALUES ('b',2,40)
  INTO elevator_max_person (elev_name,elev_order,elev_weight) VALUES ('c',3,60)
  INTO elevator_max_person (elev_name,elev_order,elev_weight) VALUES ('d',4,30)
  INTO elevator_max_person (elev_name,elev_order,elev_weight) VALUES ('e',5,30)
 SELECT * FROM dual
  ;
  
  select * from 
  (select elev_name,elev_order,elev_weight,max_weight_gain_elevator,row_number() over(  order by max_weight_gain_elevator desc )as rn from
  (select elev_name,elev_order,elev_weight,max_weight_gain_elevator from 
  (select elev_name,elev_order,elev_weight,sum(elev_weight) over(order by elev_order  ) as max_weight_gain_elevator from 
  elevator_max_person) where max_weight_gain_elevator<=200)) where rn=1
  

-------- other way

 select elev_name,elev_order,elev_weight,sum_elev  from
 (select elev_name,elev_order,elev_weight,sum(elev_weight) over(order by elev_order ) as sum_elev from elevator_max_person)   where sum_elev<=200 order by SUM_ELEV desc  
 FETCH FIRST 1 ROWS ONLY;

  
  
   where max_weight_gain_elevator<=200
  
  SELECT Id, StudentName, StudentGender, StudentAge,
SUM (StudentAge) OVER (ORDER BY Id) AS RunningAgeTotal
FROM Students



