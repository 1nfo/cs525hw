# Project 4

Qian Wang,   
Shile Zhao

### Q1 spatial join

We divided the whole space into small blocks. At the mapping phrase, each mapper will scan the points and rectangles and assign them a two-dimension id as key. Specifically, the points will be sent to reducers according to which block it belongs to; and one rectangle might be sent to different blocks since it might cover more than one block.
>hadoop jar ./Q1.jar Q1 input/P input/R out/Q1 1,1,100,100

***flexible block size with window selection***:  
We set a parameter called BLOCK_SIZE, which is the width and length of a block. Given a window parameters, e.g. left-top point and right-bottom point, firstly, we can get the list of rectangles which cross the window. Secondly, given the window parameters, we can get the list of blocks (represented by the left-top point of the block) which cross the window.

Command:
>hadoop jar ./Q1.jar Q1 input/P input/R out/Q1f 1,1,1000,1000

### Q2 CustomInputFormat
CustomInputFormat will ignore any string out of "{" , "}".
It will match values with pre-defined keys(ID, name, addr,...), i.e., the order of keys does not matter.
The class will match five keys, and if there is no match, "XXX" will be returned, which is not possible for our dataset.  
Command:
>hadoop jar ./Q2.jar Q2 input/Json.txt out/Q2


### Q3 KMeans
#### Data Input
The data input are two files uploaded on the hdfs. One file is stored points to be clusterd, the other is stored the
initial cluster centers. Both two files will be loaded into mappers, where points are loaded through FileInputFormat and centers are loaded through DistributedCache. Similar to Map-Side-Join, since centers data are very small each mapper will receive one copy.
#### Mapper
Mapper's job here is to determine which center is the closest one for each point. Concretely, at the setup stage, all the centers will be loaded through distributed caches, find out the closest centers, and send the centers' index as the key and point itself as value.
#### Reducer
Reducer will re-compute the new representives.
In the cleanup stage, it will check if centers are changed. Then the reducer will generate output as: key is (x,y), value is whether this centroid is converged.
#### Combiner
Combiner will keep the sum of coordinates and count the points for reduce stage.
#### Stop Iteration Condition
At the beginning of each job, we will first check the last output of centroid. If all the centroid are converged, then the Iteration will be stopped. Or if the interation reach to the maxmum iteration number, the iteration will be stopped as well.
Command:
>hadoop jar ./Q3.jar Q3 input/KMPoints.txt input/seeds.txt out/Q3/iter out/Q3/final_index
