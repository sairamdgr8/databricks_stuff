CREATE TABLE seq_gen ( 
    ID varchar(2), 
    seq  int 
	)
	
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
   SELECT 1 FROM DUAL
   

select id, 
      case when min(seq)=max(seq) 
       then Null 
       else 
       min(seq) end as min_seq, 
       max(seq) as max_seq 
from        
       
(select id,seq,seq-seq_rn as seq_1 from (select id,seq,row_number() over(  order by seq  ) as seq_rn from seq_gen)) 
group by id,seq_1 
order by 1 