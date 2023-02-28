--- create a percentge for top 5 marks of subject of each student

--- create a percentge for top 5 marks of subject of each student

CREATE TABLE student_percentage_question (
    ID int,
    English  int,
	Maths  int,
	Science  int,
	Geography int,
	History  int,
	Sanskrit int
);

-- drop table employee_supervior_question

INSERT INTO student_percentage_question (ID, English, Maths,Science,Geography,History,Sanskrit)
VALUES (1,85,99,92,84,84,99);
INSERT INTO student_percentage_question (ID, English, Maths,Science,Geography,History,Sanskrit)
VALUES (2,81,82,83,84,95,96);
INSERT INTO student_percentage_question (ID, English, Maths,Science,Geography,History,Sanskrit)
VALUES (3,75,55,75,75,55,75);
INSERT INTO student_percentage_question (ID, English, Maths,Science,Geography,History,Sanskrit)
VALUES (4,82,82,82,82,82,82);
INSERT INTO student_percentage_question (ID, English, Maths,Science,Geography,History,Sanskrit)
VALUES (5,83,99,45,88,76,89);


select Id ,English ,Maths ,Science ,Geography ,History ,Sanskrit , 
((English+Maths+Science+Geography+History+Sanskrit)-least(English,Maths,Science,Geography,History,Sanskrit) ) /500*100
as Percentage  from student_percentage_question

--- this query shows to remove the lowest marks of each student from each row
select least(English,Maths,Science,Geography,History,Sanskrit) from student_percentage_question


+---+-------+-----+-------+---------+-------+--------+
| ID|English|Maths|Science|Geography|History|Sanskrit|
+---+-------+-----+-------+---------+-------+--------+
|  1|     85|   99|     92|       84|     84|      99|
|  2|     81|   82|     83|       84|     95|      96|
|  3|     75|   55|     75|       75|     55|      75|
|  4|     82|   82|     82|       82|     82|      82|
|  5|     83|   99|     45|       88|     76|      89|
+---+-------+-----+-------+---------+-------+--------+