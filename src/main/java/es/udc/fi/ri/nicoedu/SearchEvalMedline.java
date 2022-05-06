package es.udc.fi.ri.nicoedu;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;

public class SearchEvalMedline {
    public static final int FIRST_Q = 1, LAST_Q = 30;
    public static final String FIELD_C = "contents";

    public static Query[] parseQueries(QueryParser parser, Path file, int q1, int q2)
            throws IOException, ParseException {

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            int nqueries = q2-q1+1;
            Query[] queries = new Query[nqueries];
            String line = reader.readLine();
            StringBuilder queryLine = new StringBuilder();
            int q = 0;

            while (line != null && !line.isEmpty() && !(line.startsWith(".I ") &&
                    Integer.parseInt(line.substring(3)) >= q1)) {
                line = reader.readLine();
            }
            reader.readLine();
            line = reader.readLine();

            for (; q < nqueries && line != null && !line.isEmpty(); line = reader.readLine()) {
                if (line.startsWith(".I ")) {
                    if (Integer.parseInt(line.substring(3)) != q1 + q + 1) {
                        return queries;
                    } else {
                        reader.readLine();
                        queries[q++] = parser.parse(queryLine.toString());
                        queryLine.setLength(0);
                    }
                } else {
                    queryLine.append(" ").append(line);
                }
            }
            if (line == null || line.isEmpty()) {
                queries[q] = parser.parse(queryLine.toString());
            }
            return queries;
        }
    }

    public static List<String>[] parseRelDocs(Path file, int q1, int q2) throws IOException {

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            int nqueries = q2-q1+1;
            List<String>[] queriesRelDocs = new ArrayList[nqueries];
            String[] tuple;
            String line = reader.readLine();

            for (int q = 0; q < nqueries; q++) {
                queriesRelDocs[q] = new ArrayList<>();
            }
            while (!line.isEmpty()) {
                tuple = line.split(" ");

                if (Integer.parseInt(tuple[0]) >= q1) {
                    break;
                }
                line = reader.readLine();
            }
            for (int q = 0; q < nqueries && line != null && !line.isEmpty();
                 line = reader.readLine()) {
                tuple = line.split(" ");

                if (Integer.parseInt(tuple[0]) > q1 + q) {
                    q++;
                }
                if (q < nqueries) {
                    queriesRelDocs[q].add(tuple[2]);
                }
            }

            return queriesRelDocs;
        }
    }

    private static void printText(String text, PrintStream txtoutput) {
        System.out.println(text);
        if (txtoutput != null) {
            txtoutput.println(text);
        }
    }

    private static void printRowCSV(String[] colVals, PrintStream csvoutput) {
        if (csvoutput != null) {
            StringBuilder row = new StringBuilder();
            int s = 0;

            for (; s < colVals.length-1; s++) {
                row.append(colVals[s]).append(",");
            }
            row.append(colVals[s]);

            csvoutput.println(row);
        }
    }

    public static void main(String[] args) {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-indexin INDEX_PATH] [-search jm Nº|tfidf]\n"
                + " [-cut Nº] [-top Nº] [-queries all|Nº| Nº-Nº]\n"
                + " [-queriesfile FILE] [-reldocsfile FILE]\n"
                + " [-outputdir PATH]\n\n";
        String indexPath = null;
        Similarity model = null;
        Integer n = null, m = null, q1 = null, q2 = null;
        String fileName = null, outputDir = null, queriesFile = null, relDocsFile = null;
        float lambda = 0f;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-search":
                    switch (args[++i]) {
                        case "jm":
                            lambda = Float.parseFloat(args[++i]);
                            model = new LMJelinekMercerSimilarity(lambda);
                            break;
                        case "tfidf":
                            model = new ClassicSimilarity();
                            break;
                        default:
                            throw new IllegalArgumentException("unknown parameter " + args[i]);
                    }
                    break;
                case "-cut":
                    n = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    m = Integer.parseInt(args[++i]);
                    break;
                case "-queries":
                    if (args[++i].equals("all")) {
                        q1 = FIRST_Q; q2 = LAST_Q;
                    } else {
                        String[] nums = args[i].split("-");

                        if (nums.length == 1) {
                            q1 = q2 = Integer.parseInt(nums[0]);
                        } else if (nums.length == 2){
                            q1 = Integer.parseInt(nums[0]);
                            q2 = Integer.parseInt(nums[1]);
                        } else {
                            throw new IllegalArgumentException("unknown parameter " + args[i]);
                        }
                    }
                    break;
                case "-outputdir":
                    outputDir = args[++i];
                    break;
                case "-queriesfile":
                    queriesFile = args[++i];
                    break;
                case "-reldocsfile":
                    relDocsFile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexPath == null || n == null || m == null || q1 == null ||
                q2 == null || queriesFile == null || relDocsFile == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (outputDir != null) {
            fileName = "medline." + (model instanceof ClassicSimilarity ? "tfidf" : "jm") + "." + n +
                    ".hits." + (model instanceof ClassicSimilarity ? "" : "lambda." + lambda) +
                    "." + (q1 == FIRST_Q && q2 == LAST_Q ? "qall" : "q1-" + q1 + (q1.equals(q2) ? "" : ".q2-" + q2));
        }

        try (Directory indexDir = FSDirectory.open(Path.of(indexPath));
             IndexReader indexReader = DirectoryReader.open(indexDir);
             PrintStream txtoutput = outputDir != null? new PrintStream(Files.newOutputStream(
                     Path.of(outputDir+fileName+".txt"))): null;
             PrintStream csvoutput = outputDir != null? new PrintStream(Files.newOutputStream(
                     Path.of(outputDir+fileName+".csv"))): null) {

            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            indexSearcher.setSimilarity(model);
            QueryParser parser = new QueryParser(FIELD_C, new StandardAnalyzer());
            Query[] queries = parseQueries(parser, Path.of(queriesFile), q1, q2);
            TopDocs topDocs;
            double[] PAns = new double[queries.length], RecallAns = new double[queries.length],
                    APAns = new double[queries.length];
            int nRel = 0, APAn = 0, nqueriesRel = 0;
            List<String>[] queriesRelDocs = parseRelDocs(Path.of(relDocsFile), q1, q2);
            double MPAn, MRecallAn, MAPAn;

            printRowCSV(new String[] {"QueryID", "P@"+n, "Recall@"+n, "AP@"+n}, csvoutput);

            for (int q = 0; q < queries.length; q++) {
                topDocs = indexSearcher.search(queries[q], n);

                printText("Query: " + (q+q1) + "\n"
                        + queries[q].toString().replaceAll("contents:", "| ") + "\n",
                        txtoutput);

                for (int i = 0; i < Math.min(n, topDocs.totalHits.value); i++) {
                    String da = indexReader.document(topDocs.scoreDocs[i].doc).get("docIDMedline");

                    if (i < m) {
                        printText("idLucene: " + topDocs.scoreDocs[i].doc + ", score: "
                                + topDocs.scoreDocs[i].score + ", idMedline: "
                                + indexReader.document(topDocs.scoreDocs[i].doc).get("docIDMedline"),
                                txtoutput);
                    }
                    if (queriesRelDocs[q].stream().anyMatch(da::equals)) {
                        nRel++;
                        APAn += nRel/(i+1);
                    }
                }
                if (nRel > 0) {
                    nqueriesRel++;
                }
                printText("", txtoutput);

                PAns[q] = ((double) nRel)/n;
                RecallAns[q] = ((double) nRel)/queriesRelDocs[q].size();
                APAns[q] = ((double) APAn)/queriesRelDocs[q].size();

                printText("P@n: " + PAns[q] + ", Recall@ns: "
                        + RecallAns[q] + ", AP@n: " + APAns[q] + "\n\n", txtoutput);

                printRowCSV(new String[] {String.valueOf(q+q1), String.valueOf(PAns[q]),
                        String.valueOf(RecallAns[q]), String.valueOf(APAns[q])}, csvoutput);
            }
            printText("Nº de queries con resultados relevantes: " + nqueriesRel,
                    txtoutput);
            MPAn = DoubleStream.of(PAns).sum()/nqueriesRel;
            MRecallAn = DoubleStream.of(RecallAns).sum()/nqueriesRel;
            MAPAn = DoubleStream.of(APAns).sum()/nqueriesRel;
            printText("MP@n: " + MPAn + ", MRecall@ns: " + MRecallAn
                            + ", MAP@n: " + MAPAn + "\n", txtoutput);

            printRowCSV(new String[] {q1+"-"+q2, String.valueOf(MPAn),
                    String.valueOf(MRecallAn), String.valueOf(MAPAn)}, csvoutput);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
