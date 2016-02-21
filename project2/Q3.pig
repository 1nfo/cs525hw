tra = LOAD 'input/Transactions' USING PigStorage(',') as (tid:int,cid:int,total:float,item:int,desc:chararray);
cus = LOAD 'input/Customers' USING PigStorage(',') as (cid:int,name:chararray,age:int,code:int,salary:float);
joined = JOIN tra BY cid, cus BY cid;
grp = GROUP joined BY code;
flt = FOREACH grp GENERATE flatten($0), COUNT($1), MAX(joined.total), MIN(joined.total);
STORE flt INTO 'out/pig3' USING PigStorage(',');
