import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class generateQ1 {
    public static void main(String[] args){
        try {
            // 100 P and 50 R are both 1K, and 2^17=128*1024 > 100M
            // e=0 to generate small data set to test
            int e=17;
            execute("P",100<<e,"R",50<<e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void execute(String name1, int size1, String name2, int size2)throws IOException{
        FileWriter fwp = new FileWriter(name1);
        BufferedWriter bwp = new BufferedWriter(fwp);
        for(int i=0;i<size1;i++){
            bwp.write(generateP());
            bwp.newLine();
        }
        bwp.close();

        FileWriter fwr = new FileWriter(name2);
        BufferedWriter bwr = new BufferedWriter(fwr);
        for(int i=0;i<size2;i++){
            bwr.write(generateR(i+1));
            bwr.newLine();
        }
        bwr.close();
    }

    static String generateP(){
        int range = 10000;
        String sep = ",";
        Random r = new Random();
        return Integer.toString(r.nextInt(range) + 1) + sep + Integer.toString(r.nextInt(range) + 1);
    }

    static String generateR(int i){
        int range = 10000, h = 20, w = 5;

        String sep = ",";
        Random r = new Random();
        return "r"+Integer.toString(i)+sep+Integer.toString(r.nextInt(range) + 1)+sep
                +Integer.toString(r.nextInt(range) + 1) + sep + Integer.toString(r.nextInt(w) + 1)
                + sep + Integer.toString(r.nextInt(h) + 1);
    }
}
