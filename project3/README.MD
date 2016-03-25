# Project 4
--

### Q1 spatial join

We divided the whole space into small blocks. At the mapping phrase, each mapper will scan the points and rectangles and assign them a two-dimension id as key. Specifically, the points will be sent to reducers according to which block it belongs to; and one rectangle might be sent to different blocks since it might cover more than one block.

***Assumption***:  
As for the **Window** parameter, we assume it will be greater than any rectangle size. In this case And the blocks are greater than any rectangle as well, so one rectangle will be sent to at most 4 different reducers.

We also assume that rectangle whose width is w (Including the edges) will has w+1 points. The same thing happens to height. 

For example, a rectangle left-down point is (1,1), with width 2 and height 3, will have a range of x, 1 ~ 3, and y, 1 ~ 4, so in all, it (2+1)*(3+1) = 12 points.

Window filtering will make the mapper jump the points  out of window. Since we have assumptions above, we only need to check the left-down and right-top points of a rectangle.

### Q2 CustomInputFormat
