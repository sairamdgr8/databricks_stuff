CREATE TABLE employee_rolling_average_question (
    emp_id int,
    emp_date varchar(255),
    emp_expense int
);

-- drop table employee_rolling_average_question

INSERT  ALL
  INTO employee_rolling_average_question (emp_id, emp_date, emp_expense) VALUES (1,'Jan-20',100)
  INTO employee_rolling_average_question (emp_id, emp_date, emp_expense) VALUES (1,'Feb-20',50)
  INTO employee_rolling_average_question (emp_id, emp_date, emp_expense) VALUES (1,'Mar-20',42)
  INTO employee_rolling_average_question (emp_id, emp_date, emp_expense) VALUES (2,'Jan-20',100)
  INTO employee_rolling_average_question (emp_id, emp_date, emp_expense) VALUES (2,'Feb-20',40)
SELECT * FROM dual;

select emp_id, emp_date, emp_expense,
avg(emp_expense) over (ORDER BY emp_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as avg_rolling_expense
from employee_rolling_average_question

select emp_id, emp_date, emp_expense,rn,avg(EMP_EXPENSE) over (order by rn) as avg_rn from  
(select emp_id, emp_date, emp_expense,row_number() over(order by emp_id) as rn from employee_rolling_average_question);
