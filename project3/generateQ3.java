import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class generateQ3 {
    public static void main(String[] args) throws IOException {
        int size = 100<<17, K = 10;
        BufferedWriter bf = new BufferedWriter(new FileWriter("KMeans.txt"));
        for (int i = 0; i < size;i++) {
            bf.write(generateQ1.generateP() + "\n");
        }
        bf.close();
        BufferedWriter bf2 = new BufferedWriter(new FileWriter("seeds.txt"));
        for (int i = 0; i < K;i++){
            bf2.write(generateQ1.generateP() + "\n");
        }
        bf2.close();
    }

}