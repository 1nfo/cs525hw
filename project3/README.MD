# Project 4

Qian Wang,   
Shile Zhao

### Q1 spatial join

We divided the whole space into small blocks. At the mapping phrase, each mapper will scan the points and rectangles and assign them a two-dimension id as key. Specifically, the points will be sent to reducers according to which block it belongs to; and one rectangle might be sent to different blocks since it might cover more than one block.
>hadoop jar ./Q1.jar Q1 input/P input/R out/Q1 1,1,100,100

***Assumption***:  
As for the **Window** parameter, we assume it will be greater than any rectangle size. In this case And the blocks are greater than any rectangle as well, so one rectangle will be sent to at most 4 different reducers.

We also assume that rectangle whose width is w (Including the edges) will has w+1 points. The same thing happens to height. 

For example, a rectangle left-down point is (1,1), with width 2 and height 3, will have a range of x, 1 ~ 3, and y, 1 ~ 4, so in all, it (2+1)*(3+1) = 12 points.

Window filtering will make the mapper jump the points  out of window. Since we have assumptions above, we only need to check the left-down and right-top points of a rectangle.

### Q2 CustomInputFormat
>hadoop jar ./Q2.jar Q2 input/Json.txt out/Q2

CustomInputFormat will ignore any string out of "{" , "}".
It will match values with pre-defined keys(ID, name, addr,...), i.e., the order of keys does not matter.
The class will match five keys, and if there is no match, "XXX" will be returned, which is not possible for our dataset.
