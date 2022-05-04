package es.udc.fi.ri.nicoedu;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Compare {
    private static double[] getResult(String result){
        List<Double> muestra = null;

        try (BufferedReader bfRead = new BufferedReader(new FileReader(result))){
            muestra = new ArrayList<Double>();
            String linea = null;

            while ((linea = bfRead.readLine()) != null){
                muestra.add(Double.parseDouble(linea.split(",")[1]));
            }
        }catch (IOException e){
            System.out.println("Error");
            e.printStackTrace();
        }
        double[] array = new double[muestra.size()];

        for (int i = 0; i < muestra.size(); i++){
            array[i] = muestra.get(i).doubleValue();
        }
        return array;
    }

    public static void main(String[] args) {
        String usage = "Compare <-results results1 results2> <-test t|wilcoxon alpha>";
        String results1 = null;
        String results2 = null;
        String test = null;
        double alpha = 0;

        if (args.length != 6){
            System.err.println("Usage: "+usage);
            System.exit(1);
        }
        for (int i = 0; i < args.length; i++){
            if ("-results".equals(args[i])){
                results1 = args[++i];
                results2 = args[++i];
            } else if ("-test".equals(args[i])) {
                test = args[++i];
                alpha = Double.parseDouble(args[++i]);
            }
        }
        double[] muestra1 = getResult(results1);
        double[] muestra2 = getResult(results2);
        double p = 0.0;

        if (test.equals("t")){
            TTest t = new TTest();
            p = t.pairedTTest(muestra1, muestra2);
        } else if (test.equals("wilcoxon")) {
            WilcoxonSignedRankTest wilcoxon = new WilcoxonSignedRankTest();
            p = wilcoxon.wilcoxonSignedRankTest(muestra1, muestra2, false);
        }else {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        System.out.println("p-valor: " +p);
        System.out.println("alpha: " + alpha);
        System.out.println("\nThe result "+(p<alpha? "has" : "hasn't") + "statistical significate");
    }
}
