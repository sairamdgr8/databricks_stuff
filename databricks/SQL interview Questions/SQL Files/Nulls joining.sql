----  null s used for left anf inner and outr joins

CREATE TABLE employee1 (
    emp_id int  ,
    emp_name varchar(255),
    emp_salary int
);

CREATE TABLE employee2 (
    emp_id int  ,
    emp_name varchar(255),
    emp_salary int
);

-- drop table employee1;
-- drop table employee2;


INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (1, 'sai', 400000);
INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (1, 'ram', 250500);
INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', Null);


INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (1, 'avi', 900000);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (1, 'ram', 660500);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', 78963);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', 987963);



select * from employee1 emp1  join employee2 emp2 on emp1.emp_id=emp2.emp_id

select * from employee1 emp1  inner join employee2 emp2 on emp1.emp_id=emp2.emp_id 

select * from employee1 emp1  left join employee2 emp2 on emp1.emp_id=emp2.emp_id --- nulls doesn't join

----  null s used for left anf inner and outr joins

CREATE TABLE employee1 (
    emp_id int  ,
    emp_name varchar(255),
    emp_salary int
);

CREATE TABLE employee2 (
    emp_id int  ,
    emp_name varchar(255),
    emp_salary int
);

-- drop table employee1;
-- drop table employee2;


INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (1, 'sai', 400000);
INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (1, 'ram', 250500);
INSERT INTO employee1 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', Null);


INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (1, 'avi', 900000);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (1, 'ram', 660500);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', 78963);
INSERT INTO employee2 (emp_id, emp_name, emp_salary)
VALUES (null, 'pl', 987963);



select * from employee1 emp1  join employee2 emp2 on emp1.emp_id=emp2.emp_id

select * from employee1 emp1  inner join employee2 emp2 on emp1.emp_id=emp2.emp_id 

select * from employee1 emp1  left join employee2 emp2 on emp1.emp_id=emp2.emp_id --- nulls doesn't join


select * from employee1 emp1  left outer join employee2 emp2 on emp1.emp_id=emp2.emp_id  ---- left join and left outer join bothe are same


select * from employee1 emp1  full outer join employee2 emp2 on emp1.emp_id=emp2.emp_id


select * from employee1 emp1  cross join employee2 emp2  --- irrespective of constrain it do's cartsian product






select * from employee1 emp1  right join employee2 emp2 on emp1.emp_id=emp2.emp_id


select * from employee1 emp1  full outer join employee2 emp2 on emp1.emp_id=emp2.emp_id


select * from employee1 emp1  cross join employee2 emp2  --- irrespective of constrain it do's cartsian product



