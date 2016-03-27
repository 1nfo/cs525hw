/**
 * Created by wangqian on 3/25/16.
 */
class Rect{
    String id;
    int width = 0;
    int height = 0;
    int x_lt = 0;
    int y_lt = 0;
    int x_rb = 0;
    int y_rb = 0;

    public Rect(String id, int x_lt,int y_lt, int width, int height){
        this.id = id;
        this.width = width;
        this.height = height;
        this.x_lt = x_lt;
        this.y_lt = y_lt;
        this.x_rb = x_lt + width;
        this.y_rb = y_lt + height;
    }
}

class Window{
    int x_lt = 0;
    int y_lt = 0;
    int x_rb = 0;
    int y_rb = 0;

    public Window(int x_lt, int y_lt, int x_rb, int y_rb){
        this.x_lt = x_lt;
        this.y_lt = y_lt;
        this.x_rb = x_rb;
        this.y_rb = y_rb;
    }
}