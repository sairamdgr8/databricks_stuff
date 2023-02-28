SELECT SQL_TEXT,FIRST_LOAD_TIME,LAST_LOAD_TIME FROM V$SQL V where  to_date(v.FIRST_LOAD_TIME,'YYYY-MM-DD hh24:mi:ss') > sysdate - 60

CREATE TABLE employees_details (
    emp_id int not null primary key,
    emp_name varchar(255),
    emp_salary int,
	emp_mobile varchar(255)
);


INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (1, 'sai', 400000,'asus');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (2, 'ram', 250500,'redmi');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (3, 'pl', Null,'asus');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (4, 'avi', Null,'samsung');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (5, 'pramod', 999666,'xiamoi');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (3, 'pl', Null,'asus');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (4, 'avi', Null,'redmi');
INSERT INTO employees_details (emp_id, emp_name, emp_salary,emp_mobile) VALUES (5, 'pramod', 999666 'vivo');

drop table  employees_details;





========

select * from  employees_details 
where 
lower(emp_mobile) like '%asus%' OR
lower(emp_mobile) like '%redmi%' OR
lower(emp_mobile) like '%vivo%' OR
lower(emp_mobile) like '%samsung%' 

----


select * from  employees_details 
where 
REGEXP_LIKE(lower(emp_mobile),'asus|redmi|samsung|vivo')

