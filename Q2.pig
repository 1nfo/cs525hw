tra = LOAD 'input/Transactions' USING PigStorage(',') as (tid:int,cid:int,total:float,item:int,desc:chararray);
cus = LOAD 'input/Customers' USING PigStorage(',') as (cid:int,name:chararray,age:int,code:int,salary:float);
cl1 = FOREACH tra GENERATE cid, total, item;
cl2 = FOREACH cus GENERATE cid, name, salary;
grp = GROUP cl1 BY cid;
flt = FOREACH grp GENERATE flatten($0) as cid, COUNT($1) as count, SUM(cl1.total) as total, MIN(cl1.item) as min;
res = JOIN cl2 by cid, flt by cid;
res2 = FOREACH res GENERATE  flt::cid, name, salary, count, total, min;
STORE res2 INTO 'out/pig2' USING PigStorage(',');
