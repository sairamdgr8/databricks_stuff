1.) use the above transactions dataset, come up with a sql query to get the most recent/latest balance, 
transaction for each account number.

acc_num	transaction_time 	transact_id	balance
	    
123	    1/1/2019 8:00		101		1000
123	    1/1/2019 8:00		102		2000
123	    1/1/2019 8:00		103		3000
789	    1/1/2019 8:00		104		1000
789	    1/1/2019 8:00		105		500
123	    1/1/2019 8:00		106		4000

expected output

acc_num	transa_id 	balance
123	     106		4000
789	     105		500


------------




Create table recent_transaction( 
acc_num int, 
transaction_time varchar(20), 
transact_id int,
balance int

);

-- drop table recent_transaction

INSERT ALL    
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (123,'1/1/2019 8:00',101,1000)
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (123,'1/1/2019 8:00',102,2000)
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (123,'1/1/2019 8:00',103,3000)
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (789,'1/1/2019 8:00',104,1000)
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (789,'1/1/2019 8:00',105,500)
  INTO recent_transaction (acc_num,transaction_time,transact_id,balance) VALUES (123,'1/1/2019 8:00',106,4000)
 SELECT * FROM dual ;
 
 select * from (select acc_num,transaction_time,transact_id,balance ,row_number() over(partition by acc_num order by transact_id desc ) as rn from recent_transaction )where rn=1