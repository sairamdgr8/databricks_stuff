CREATE table cricketmatch  (  
    teamname varchar(255)
);

INSERT INTO cricketmatch (teamname) 
VALUES ('Austrlia');
INSERT INTO cricketmatch (teamname) 
VALUES ('Pakistan');
INSERT INTO cricketmatch (teamname) 
VALUES ('India');
INSERT INTO cricketmatch (teamname) 
VALUES ('Srilanka');

select a.teamname||' V/s '||b.teamname as team from 
cricketmatch a join cricketmatch b 
on a.teamname<b.teamname



-- drop table cricketmatch

