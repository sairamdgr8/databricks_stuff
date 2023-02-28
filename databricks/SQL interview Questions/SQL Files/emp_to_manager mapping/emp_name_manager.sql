CREATE table emp_name_magr   (  
    id int,  
    emp_name varchar(255),  
    manager_id int  
);


INSERT INTO emp_name_magr (id, emp_name, manager_id) VALUES (1,'Sai',3);
INSERT INTO emp_name_magr (id, emp_name, manager_id) VALUES (2,'prem',4);
INSERT INTO emp_name_magr (id, emp_name, manager_id) VALUES (3,'chaitanya',1);
INSERT INTO emp_name_magr (id, emp_name, manager_id) VALUES (4,'mahesh',2);


select * from emp_name_magr

select e1.emp_name,e2.emp_name from emp_name_magr e1 join emp_name_magr e2 on e1.id=e2.manager_id;


