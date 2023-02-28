CREATE TABLE employees (
    emp_id int not null primary key,
    emp_name varchar(255),
    emp_salary int
);


CREATE TABLE orders (
    order_id int not null primary key ,
    order_name varchar(255),
    order_sales int
);

--drop table employees;
--drop table orders;

INSERT INTO employees (emp_id, emp_name, emp_salary)
VALUES (1, 'sai', 400000);
INSERT INTO employees (emp_id, emp_name, emp_salary)
VALUES (2, 'ram', 250500);
INSERT INTO employees (emp_id, emp_name, emp_salary)
VALUES (3, 'pl', Null);
INSERT INTO employees (emp_id, emp_name, emp_salary)
VALUES (4, 'avi', Null);
INSERT INTO employees (emp_id, emp_name, emp_salary)
VALUES (5, 'pramod', 999666);



INSERT INTO orders (order_id, order_name, order_sales)
VALUES (1, 'A-7000', 5);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (2, 'A-7001', 6);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (3, 'A-70002', Null);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (4, 'A-70003', 18);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (5, 'A-70004', 29);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (6, 'A-70005', Null);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (7, 'A-700011', 2563);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (8, 'A-70006', 119);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (9, 'A-70007', 102);
INSERT INTO orders (order_id, order_name, order_sales)
VALUES (10, 'A-70008', Null);


select * from employees join orders on emp_id=order_id;

select * from employees inner join orders on emp_id=order_id;

select * from employees left join orders on emp_id=order_id;

select * from employees left outer join orders on emp_id=order_id;
select * from employees  join orders on emp_id=order_id;

select * from employees right join orders on emp_id=order_id;
select * from employees right outer join orders on emp_id=order_id;


select * from employees outer join orders on emp_id=order_id;

select * from employees full outer join orders on emp_id=order_id;

select * from employees cross join orders on emp_id=order_id;


