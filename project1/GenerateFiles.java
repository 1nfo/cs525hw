import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by hadoop on 1/30/16.
 */
public class GenerateFiles {
    public static void main(String[] args) throws IOException {
        execute("Customers", "Transactions");
    }

    public static void execute(String path1, String path2) throws IOException {
        String sep = ",";
        Random r = new Random();
        int cNum = 50000;
        int tNum = 5000000;

        // dataset 1
        FileWriter fstream = new FileWriter(path1);
        BufferedWriter out = new BufferedWriter(fstream);
        for (int i = 1; i <= cNum; i++) {
            // cid
            String line = Integer.toString(i);
            // name
            line += sep + generateNames(r.nextInt(10) + 1);
            // age
            line += sep + Integer.toString(r.nextInt(61) + 10);
            // country
            line += sep + Integer.toString(r.nextInt(10)+1);
            // salary
            line += sep + Float.toString(r.nextFloat() * 9900 + 100);
            out.write(line);
            out.newLine();
        }
        out.close();

        // dataset 2
        int[] idMapping = generateMapping(tNum,cNum);
        FileWriter fstream2 = new FileWriter(path2);
        BufferedWriter out2 = new BufferedWriter(fstream2);
        for (int i = 0; i < tNum; i++) {
            // tid
            String line = Integer.toString(i+1);
            // cid
            line += sep + Integer.toString(idMapping[i]);
            // total
            line += sep + Float.toString(r.nextFloat() * 990 + 10);
            // number
            line += sep + Integer.toString(r.nextInt(10) + 1);
            // age
            line += sep + generateString(r.nextInt(31));
            out2.write(line);
            out2.newLine();
        }
        out2.close();
    }

    public static String generateNames(int size_minues_ten) {
        Random r = new Random();
        int sep = r.nextInt(size_minues_ten + 2) + 4;
        String res = "";
        for (int i = 0; i < sep; i++) {
            int tmp = (r.nextInt(26) + 97);
            if (i == 0) tmp -= 32;
            res += (char) tmp;
        }
        res += " ";
        for (int i = sep; i < size_minues_ten + 10; i++) {
            int tmp = (r.nextInt(26) + 97);
            if (i == sep) tmp -= 32;
            res += (char) tmp;
        }
        return res;
    }

    public static String generateString(int size_minues_twenty) {
        Random r = new Random();
        String res = "";
        for (int i = 0; i < size_minues_twenty + 20; i++) {
            int tmp = (r.nextInt(26) + 97);
            res += (char) tmp;
        }
        return res;
    }

    public static int[] generateMapping(int m, int n) {
        Random r = new Random();
        int avg = m / n;
        int[] res = new int[m];
        for (int i = 0, k = 0; i < n; i++) {
            for (int j = 0; j < avg; j++) {
                res[k++] = i+1;
            }
        }
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < avg; j++) {
                int tmp = res[i], rpos = r.nextInt(m);
                res[i] = res[rpos];
                res[rpos] = tmp;
            }
        }
        return res;
    }
}
