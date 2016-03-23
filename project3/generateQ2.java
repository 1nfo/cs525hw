import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class generateQ2 {
    public static void main(String[] args) throws IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter("Json.txt"));
        int N=4<<17,count=0;
        for (int i=0;i<N;i++){
            if (count++!=0){
                bf.write(",\n");
            }
            bf.write(getTuple(i+1));
        }
        bf.close();
    }

    static String getTuple(int id){
        return "{"+getId(id)+getName()+getAddr()+getSalary()+getGender()+"}";
    }

    static String getId(int id){
        return " Customer ID: "+Integer.toString(id)+",\n";
    }

    static String getName(){
        Random r = new Random();
        String res="";
        for (int i=0;i<100;i++){
            res += (char) (r.nextInt(26)+97);
        }
        return "  Name: "+res+",\n";
    }

    static String getAddr(){
        Random r = new Random();
        String res="";
        for (int i=0;i<100;i++){
            res += (char) (r.nextInt(26)+97);
        }
        return "  Address: "+res+",\n";
    }

    static String getSalary(){
        Random r = new Random();
        return "  Salary: "+Integer.toString(r.nextInt(901)+100)+",\n";
    }

    static String getGender(){
        Random r = new Random();
        return "  Gender: "+(r.nextBoolean()? "male":"female")+"\n";
    }
}
