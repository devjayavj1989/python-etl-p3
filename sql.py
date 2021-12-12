
createSchema=''' create schema if not exists petl3'''

createTable='''create table  if not exists petl3.viable_county(
geo_id  int,
state  text ,
county  text,
sales_vector  int 
)'''


insertTable=''' insert into petl3.viable_county values (%s,%s,%s,%s)'''