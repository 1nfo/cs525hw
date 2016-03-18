### CS 561 Advanced Topics in Database System
#### Project #1, Hadoop Map-Reduce Job
Members: Shile Zhao, Qian Wang  

--
#### 1 - Creating Datasets
We wrote a java class ***GenerateFiles*** to generate two datafiles, ***Customers*** and ***Transactions***.
Every Customer strictly has 100 transactions, and the names for customers are quoted with "(quotation mark), so that RegExp can identify the comma in the name.

--
#### 2 - Uploading to HDFS
In order to unload the two files, the commands we used are like this:

>hadoop fs -put \<src path of Customers\> input/   
>hadoop fs -pit \<src path of Transactions\> input/

The ***Customers*** file are small, so it can be stored within one block, while ***Transactions*** are large so it had been spilted into several chunks.

--
#### 3 - Map-Reduce Jobs
##### 3.1 Customers Filtering
This job is map-only job. The mapper only filter records according to the **CountryCode**.
##### 3.2 Transactions Aggregating
The second query is grouping which needs implementing with map-reduce. Since the multiple values are required between mappers and reducers, one simple solution is to put all values into one string separated with comma. The other way is to implement a new ***Writable*** class. The table (unit: ms) below is the comparison of combiner and without combiner.

 |Mapper|Reducer
--------|------|-------
combiner|19800 |1670
no-combiner|21640 |5370

Obviously, the combiner can save the running time, because it will reduce the exchanging information between the mappers and reducers.
##### 3.3 Map-Reduce Join
For this joining job, the key-value pairs between the mappers and reducers are CustomerID(key), and (table flag, other details from each tables)
One tricky part is that you can not set up the combiner.
##### 3.4 Map Side Join
Since this query requires two different keys for join part and grouping part, two works can not be completed with reducing stage.
So alternative way is to replicate one smaller table (***Customers***) for each mapper. We loaded smaller table from local file system into a hash table, then do the map join and generate the pair whose key is ***CountryCode***.

##### 3.5 DistributedCache
In mapper part, we used DistributedCache library to read the Customers information into mapper using setup function. Then join the customer and transaction using the key of custID. Next write the key is custName, value is the transID. In reducer part, we aggregate the transaction number for every customer. Then we override the cleanup function in reducer, after comparation the transaction number for every customer then get the customers who have the least transaction number.
