create table ones_check_1
(
A int
);

create table ones_check_2
(
A int
);

INSERT INTO ones_check_1 (A) VALUES (1);
INSERT INTO ones_check_1 (A) VALUES (1);
INSERT INTO ones_check_1 (A) VALUES (1);
INSERT INTO ones_check_1 (A) VALUES (1);

INSERT INTO ones_check_2 (A) VALUES (1);
INSERT INTO ones_check_2 (A) VALUES (1);
INSERT INTO ones_check_2 (A) VALUES (1);
INSERT INTO ones_check_2 (A) VALUES ('');
INSERT INTO ones_check_2 (A) VALUES ('');

-- 
drop table ones_check_2;
drop table ones_check_1;


select * from ones_check_2


select * from ones_check_1 a1  join ones_check_2 b1  on a1.A=b1.A  -------- 12 rows

select * from ones_check_1 a1  cross join ones_check_2 b1  --- 12 rows     ---- simple join and cross join are same but cross join dont require constraint

select * from ones_check_1 a1  left join ones_check_2 b1  on a1.A=b1.A   --- 12 rows 

select * from ones_check_1 a1  right join ones_check_2 b1  on a1.A=b1.A 

