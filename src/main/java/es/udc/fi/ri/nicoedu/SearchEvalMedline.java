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
    private static final int FIRST_Q = 1, LAST_Q = 30;
    private static final String FILE_Q = "/home/nico/Documentos/medQD/MED.QRY",
            FIELD_C = "contents", DIR_OUT = "/home/nico/Documentos/medBD/",
            FILE_R = "/home/nico/Documentos/medQD/MED.REL";

    private static Query[] parseQueries(QueryParser parser, Path file, int q1, int q2)
            throws IOException, ParseException {
        int nqueries = q2-q1+1;
        Query[] queries = new Query[nqueries];
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8));
        String line = reader.readLine();
        StringBuilder queryLine = new StringBuilder();
        int q = 0;

        while (!line.isEmpty() && !(line.startsWith(".I ") &&
                Integer.parseInt(line.substring(3)) == q1)) {
            line = reader.readLine();
        }
        reader.readLine();
        line = reader.readLine();

        for (; q < nqueries && !line.isEmpty(); line = reader.readLine()) {
            if (line.startsWith(".I ")) {
                if(Integer.parseInt(line.substring(3)) != q1+q+1) {
                    throw new ParseException();
                } else {
                    reader.readLine();
                    queries[q++] = parser.parse(queryLine.toString());
                    queryLine.setLength(0);
                }
            } else {
                queryLine.append(" ").append(parser.parse(line));
            }
        }
        if (line.isEmpty()) {
            queries[q] = parser.parse(queryLine.toString());
        }
        return queries;
    }

    private static List<String>[] parseRelDocs(Path file, int q1, int q2) throws IOException {
        int nqueries = q2-q1+1;
        List<String>[] queriesRelDocs = new ArrayList[nqueries];
        String[] tuple;
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8));
        String line = reader.readLine();

        for (int q = 0; q < nqueries; q++) {
            queriesRelDocs[q] = new ArrayList<>();
        }
        while (!line.isEmpty()) {
            tuple = line.split(" ");

            if(Integer.parseInt(tuple[0]) == q1) {
                break;
            }
            line = reader.readLine();
        }
        for (int q = 0; q < nqueries && !line.isEmpty(); line = reader.readLine()) {
            tuple = line.split(" ");

            if (Integer.parseInt(tuple[0]) > q1+q) {
                q++;
            }
            if (q < nqueries) {
                queriesRelDocs[q].add(tuple[2]);
            }
        }

        return queriesRelDocs;
    }

    public static void main(String[] args) {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-indexin INDEX_PATH] [-search jm Nº|tfidf]\n"
                + " [-cut Nº] [-top Nº] [-queries all|Nº| Nº-Nº]\n\n";
        String indexPath = null;
        Similarity model = null;
        Integer n = null, m = null, q1 = null, q2 = null;
        String fileName;
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
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexPath == null || n == null || m == null || q1 == null || q2 == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }


        fileName = "medline." + (model instanceof ClassicSimilarity? "tfidf": "jm") + "." + n +
                ".hits." + (model instanceof ClassicSimilarity? "": "lambda." + lambda) +
                "." + (q1 == FIRST_Q && q2 == LAST_Q? "qall": "q1-" + q1 + (q1.equals(q2)? "" : ".q2-" + q2));

        try (Directory indexDir = FSDirectory.open(Path.of(indexPath));
             IndexReader indexReader = DirectoryReader.open(indexDir);
             PrintStream txtoutput = new PrintStream(Files.newOutputStream(
                     Path.of(DIR_OUT+fileName+".txt")));
             PrintStream csvoutput = new PrintStream(Files.newOutputStream(
                     Path.of(DIR_OUT+fileName+".csv")))) {

            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            indexSearcher.setSimilarity(model);
            QueryParser parser = new QueryParser(FIELD_C, new StandardAnalyzer());
            Query[] queries = parseQueries(parser, Path.of(FILE_Q), q1, q2);
            TopDocs topDocs;
            double[] PAns = new double[n], RecallAns = new double[n], APAns = new double[n];
            int nRel = 0, APAn = 0, nqueriesRel = 0;
            List<String>[] queriesRelDocs = parseRelDocs(Path.of(FILE_R), q1, q2);

            for (int q = 0; q < queries.length; q++) {
                topDocs = indexSearcher.search(queries[q], n);

                System.out.println("Query: " + (q+q1) + "\n"
                        + queries[q].toString().replaceAll("contents:", "| ") + "\n");

                for (int i = 0; i < n; i++) {
                    String da = indexReader.document(topDocs.scoreDocs[i].doc).get("docIDMedline");

                    if (i < m) {
                        System.out.println("idLucene: " + topDocs.scoreDocs[i].doc + ", score: "
                                + topDocs.scoreDocs[i].score + ", idMedline: "
                                + indexReader.document(topDocs.scoreDocs[i].doc).get("docIDMedline"));
                    }
                    if (queriesRelDocs[q].stream().anyMatch(da::equals)) {
                        nRel++;
                        APAn += nRel/(i+1);
                    }
                }
                if (nRel > 0) {
                    nqueriesRel++;
                }
                System.out.println();

                PAns[q] = ((double) nRel)/n;
                RecallAns[q] = ((double) nRel)/queriesRelDocs[q].size();
                APAns[q] = ((double) APAn)/queriesRelDocs[q].size();

                System.out.println("P@n: " + PAns[q] + ", Recall@ns: "
                        + RecallAns[q] + ", AP@n: " + APAns[q] + "\n\n");
            }
            System.out.println("Nº de queries con resultados relevantes: " + nqueriesRel);
            System.out.println("MP@n: " + DoubleStream.of(PAns).sum()/nqueriesRel
                    + ", MRecall@ns: " + DoubleStream.of(RecallAns).sum()/nqueriesRel
                    + ", MAP@n: " + DoubleStream.of(APAns).sum()/nqueriesRel);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
