raw = LOAD 'input/Transactions' USING PigStorage(',') as (tid:int,cid:int,total:float,item:int,desc:chararray);
clean = FOREACH raw GENERATE cid, total;
grp = GROUP clean BY (cid);
result = FOREACH grp GENERATE flatten($0) as CustomerID, COUNT($1) as NumTransactions, SUM(clean.total);
STORE result INTO 'out/pig1' USING PigStorage(',');
