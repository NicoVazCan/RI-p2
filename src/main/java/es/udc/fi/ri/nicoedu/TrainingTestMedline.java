package es.udc.fi.ri.nicoedu;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class TrainingTestMedline {
    private final static String FIELD = "Contents";

    private static IndexReader reader = null;
    private static IndexSearcher searcher = null;
    private static Analyzer analyzer = null;

    //Lista para guardar resultados de cada query
    private static List<Float> resultadosMetricaTrain = new ArrayList<Float>();
    private static List<Float> resultadosMetricaTest = new ArrayList<Float>();

    private static void processQuery(String queryString, String mrelString, int cut, String metrica,
                                     boolean train, int currentQuery) throws IOException, ParseException, CorruptIndexException{
        boolean relevant = false;
        QueryParser parser;
        parser = new QueryParser(FIELD, analyzer);
        int[] array = new int[cut];
        int t = 0;

        List<String> relevantes = Arrays.asList(mrelString.trim().split("\\s+"));
        Query query = parser.parse(queryString.toLowerCase()); //evitar AND

        TopDocs topDocs = searcher.search(query, cut);

        //Recorrer top de docs
        for (int i = 0; i < Math.min(cut, topDocs.totalHits.value); i++){
            relevant = relevantes.contains(reader.document(topDocs.scoreDocs[i].doc).get("DocIDMedline"));
            array[i] = relevant ? 1 : 0;
        }

        float aux = 0;
        if (metrica.equals("P")){
            //suma los valores del array usando stream
            t = Arrays.stream(array).sum();
            aux = (float) t / cut;
        }
        else if (metrica.equals("R")) {
            t = Arrays.stream(array).sum();
            aux = (float) t / relevantes.size();
        } else if (metrica.equals("MAP")) {
            float sum2 = 0;
            int sum = 0;
            int cnt = 0;
            for (int i = 0; i<cut; i++){
                sum = 0;
                if (array[i] == 1){
                    cnt ++;
                    //Calculo de media haciendo el cut
                    for (int j = 0; j<=i; j++){
                        sum += array[j];
                    }
                    sum2 += (float) sum / (i+1);
                }
            }
            aux = (cnt == 0 ? (float) 0.0 : sum2 / cnt);
        }
        if (train)
            resultadosMetricaTrain.add(aux);
        else {
            resultadosMetricaTest.add(aux);
        }
    }

    public static void readQueries(int start, int end, String queryPath, String mrelPath, int cut, String metrica,
                                   boolean train) throws IOException {
        BufferedReader bufreadQuery = null;
        BufferedReader bufreadMrel = null;
        //Leer queries
        String line = null;
        boolean ini = true;
        String queryString = null;
        int currentQuery = 0;
        //leer MED.REL
        String mrelString = null;

        try {
            bufreadQuery = new BufferedReader(new FileReader(queryPath));
            bufreadMrel = new BufferedReader(new FileReader(mrelPath));

            while ((line = bufreadQuery.readLine()) != null){
                if (line.equals("/")){
                    if (!queryString.equals("")){
                        processQuery(queryString, mrelString, cut, metrica, train, currentQuery);
                        ini = true;
                    }

                } else if (ini) {
                    currentQuery = Integer.parseInt(line);
                    queryString = "";

                    //MED.REL
                    while ((line = bufreadMrel.readLine()) != null){
                        if (line.equals("   /")){
                            break;
                        } else if (ini) {
                            mrelString = "";
                            ini = false;
                        }else mrelString += line + "";
                    }
                    ini = false;

                } else if ((end >= currentQuery) && (start <= currentQuery)) {
                    queryString += line + "";
                } else if (end < currentQuery) {
                    break;
                }
            }
            ini = true;
        }catch (IOException e){
            System.err.println("Error: " + e);
            e.printStackTrace();
        }catch (ParseException e){
            e.printStackTrace();
        }finally {
            bufreadQuery.close();
            bufreadMrel.close();
        }

    }

    public static void main(String[] args) throws Exception{
        String usage = "Usage: TrainingTestMedline [-indexin pathname] [-evaljm int1-int2 int3-int4] [-cut n]" +
                "[-metrica P | R | MAP]";
        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))){
            System.out.println(usage);
            System.exit(0);
        }
        Properties properties = new Properties();
        String docsPath = null;
        String index = null;
        int cut = 5;
        String metrica = null;
        String qTrain = null;
        String qTest = null;
        String output = null;
        boolean jm = false;
        boolean train = true;

        try {
            properties.load(new FileInputStream(new File("./src/main/resources/-config.properties")));
            docsPath = properties.getProperty("docs");
        }catch (FileNotFoundException e){
            e.printStackTrace();
            System.exit(1);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }
        for (int i = 0; i<args.length; i++){
            if ("-indexin".equals(args[i])){
                index = args[i+1];
                i++;
            } else if ("-metrica".equals(args[i])) {
                metrica = args[i+1];
                i++;
            } else if ("-cut".equals(args[i])) {
                cut = Integer.parseInt(args[i+1]);
                i++;
            } else if ("-evaljm".equals(args[i])) {
                qTrain = args[i+1];
                qTest = args[i+2];
                i += 2;
                jm = true;
            } else if ("-evaltfidf".equals(args[i])) {
                qTrain = args[i + 1];
                qTest = args[i + 2];
                i += 2;
            }
        }
        if (index == null || metrica == null || (!metrica.equals("R") && !metrica.equals("P") && !metrica.equals("MAP"))){
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        searcher = new IndexSearcher(reader);
        analyzer = new StandardAnalyzer();

        System.out.println("Searching " + index + ", with: " + metrica + " metrica\n");
        int startTrain = 0;
        int endTrain = 0;
        int startTest = 0;
        int endTest = 0;

        if (qTrain != null && qTest != null){
            String[] limit = qTrain.split("-");
            startTrain = Integer.parseInt(limit[0]);
            endTrain = Integer.parseInt(limit[1]);

            limit = qTest.split("-");
            startTest = Integer.parseInt(limit[0]);
            endTest = Integer.parseInt(limit[1]);
        }else {
            System.out.println(usage);
            System.exit(0);
        }

        String qPath = docsPath + "query-text";
        String mrelPath = docsPath + "MED.REL";

        float meta = 0;
        float bParam = 0;
        float bMeta = 0;
        float t = 0;
        float[] lamda = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f };
        float[] param = jm ? lamda:  ;

        for (float p: param) {
            if (jm){
                searcher.setSimilarity(new LMJelinekMercerSimilarity(p));
            }
            resultadosMetricaTrain = new ArrayList<Float>();
            readQueries(startTrain, endTrain, qPath, mrelPath, cut, metrica, train);

            for (float f : resultadosMetricaTrain){
                t += f;
            }
            meta = t / resultadosMetricaTrain.size();
            System.out.println("Rango de queries: " + qTrain+ ", " + (jm ? "lamda" : "") + "=" + p + "meta = " +meta);
            if (meta > bMeta){
                bParam = p;
                bMeta = meta;
            }
        }
        System.out.println("\nMejor " + (jm?"lamda": "") + ": " + bParam+ ", con una meta de: "+bMeta);
        if (jm){
            searcher.setSimilarity(new LMJelinekMercerSimilarity(bParam));
        }
        train = false;

        readQueries(startTest, endTest, qPath, mrelPath, cut, metrica, train);

        t = 0;
        for (float f : resultadosMetricaTest){
            t += f;
        }
        meta = t/resultadosMetricaTest.size();
        System.out.println("Rango del query test: " + qTest+ ", meta = " + meta);
    }
}
