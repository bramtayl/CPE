-- remove the wordpress database, create a new one, and give the wordpress user access
drop database wordpress;
create database wordpress;
grant all on wordpress.* to wordpress@localhost;
