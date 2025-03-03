create database retail_warehouse

CREATE TABLE payment_method (
    method_id SERIAL PRIMARY KEY,
    method VARCHAR(255) NOT NULL
);

create table order_status (
	status_id SERIAL primary key,
	status varchar(255) not null
);

create table product (
	product_id SERIAL primary key,
	prod_name varchar(255) not null,
	prod_brand varchar(255) not null,
	prod_category varchar(255) not null,
	prod_type varchar(255) not null
);

create table income (
	income_type SERIAL primary key,
	income_category varchar (255) not null
);

create table gender(
	gender_id SERIAL primary key,
	gender_val varchar(255) not null
);

create table location (
	location_id SERIAL primary key,
	city varchar(255),
	state varchar(255),
	zipcode varchar(255)
);

create table customer (
	cust_id SERIAL primary key,
	name varchar(255),
	email varchar(255),
	phone varchar (255),
	address varchar (255),
	location_id integer,
	age integer,
	gender_id integer,
	FOREIGN KEY (location_id) REFERENCES location(location_id),
	FOREIGN KEY (gender_id) REFERENCES gender(gender_id)
	
);
create table Time_DIM (
	date date,
	day int,
	month int,
	year int,
	day_name varchar(25)
);

create table feedback (
	feedback_id SERIAL primary key,
	feeback_value varchar(255)
);


create table shipping_method(
	method_id SERIAL primary key,
	method_name varchar(255) not null
);
ALTER TABLE shipping_method RENAME COLUMN method_id TO shipping_method_id;
alter table shipping_method rename column method_name to shipping_method_name;


create table customer_segment(
	segment_id SERIAL primary key,
	segment_name varchar(255) not null
)

create table sales (

cust_id int,
loc_id int,
feedback_id int,
gender_id int,
income_id int,
prod_id int,
status_id int,
method_id int,
total_purchases float,
amount float,
total_amount float,
FOREIGN KEY (cust_id) REFERENCES customer(cust_id),
FOREIGN KEY (loc_id) REFERENCES location(location_id),
FOREIGN KEY (feedback_id) REFERENCES feedback(feedback_id),
FOREIGN KEY (gender_id) REFERENCES gender(gender_id),
FOREIGN KEY (income_id) REFERENCES income(income_type),
FOREIGN KEY (prod_id) REFERENCES product(product_id),
FOREIGN KEY (status_id) REFERENCES order_status(status_id),
FOREIGN KEY (method_id) REFERENCES shipping_method(method_id)

);

ALTER TABLE sales ADD COLUMN segment_id INT REFERENCES customer_segment(segment_id);

ALTER TABLE sales ADD COLUMN payment_method_id INT REFERENCES payment_method(method_id);

alter table location add column country varchar(255)


SELECT pg_get_serial_sequence('customer', 'cust_id');

ALTER SEQUENCE customer_cust_id_seq RESTART WITH 1;

alter table sales add column Transaction_ID float 
alter table sales add column Ratings float
alter table sales add column Trasact_Date Date
select * from income i 

select * from sales s  
