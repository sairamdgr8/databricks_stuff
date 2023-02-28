CREATE TABLE employee_supervior_question (
    emp_id int  ,
    emp_name varchar(255),
    emp_supervisor_id int
);

-- drop table employee_supervior_question

INSERT INTO employee_supervior_question (emp_id, emp_name, emp_supervisor_id)
VALUES (1, 'avi', 200);
INSERT INTO employee_supervior_question (emp_id, emp_name, emp_supervisor_id)
VALUES (2, 'sai', 100);
INSERT INTO employee_supervior_question (emp_id, emp_name, emp_supervisor_id)
VALUES (100, 'ram', 2);
INSERT INTO employee_supervior_question (emp_id, emp_name, emp_supervisor_id)
VALUES (200, 'bunny', 2);
INSERT INTO employee_supervior_question (emp_id, emp_name, emp_supervisor_id)
VALUES (300, 'pramod', 1);


select e.emp_id, e.emp_name, es.emp_name as manager from employee_supervior_question e inner  join  employee_supervior_question es on e.emp_id=es.emp_supervisor_id

SELECT e1.name, e2.name as Manager 
       FROM Employee e1 JOIN Employee e2 ON e1.Id = e2.ManagerId
       
       

CREATE TABLE employee_supervior_question(
   EMPLOYEEID INT ,
   e_NAME VARCHAR(50),
   MANAGERID INT
);


INSERT INTO employee_supervior_question VALUES(101,'Mary',102);
INSERT INTO employee_supervior_question VALUES(102,'Ravi',NULL);
INSERT INTO employee_supervior_question VALUES(103,'Raj',102);
INSERT INTO employee_supervior_question VALUES(104,'Pete',103);
INSERT INTO employee_supervior_question VALUES(105,'Prasad',103);
INSERT INTO employee_supervior_question VALUES(106,'Ben',103);


SELECT E1.EMPLOYEEID,E1.e_NAME,E2.e_NAME AS MANAGER_NAME
FROM employee_supervior_question E1
inner JOIN employee_supervior_question E2
ON E2.EMPLOYEEID =E1.MANAGERID 

--------------------------


