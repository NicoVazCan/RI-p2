package es.udc.fi.ri.nicoedu;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

public class SearchEvalMedline {
    private static final int FIRST_Q = 1, LAST_Q = 30;
    private static final String FILE_Q = "/home/nico/Documentos/medQD/MED.QRY",
            FIELD_Q = "contents", DIR_OUT = "/home/nico/Documentos/medBD/";

    private static Query[] parseQueries(QueryParser parser, Path file, int q1, int q2)
            throws IOException, ParseException {
        int nqueries = q2-q1+1;
        Query[] queries = new Query[nqueries];
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8));
        String line = reader.readLine();
        StringBuilder queryLine = new StringBuilder();

        while (!line.isEmpty() && !(line.startsWith(".I ") &&
                Integer.parseInt(line.substring(3)) == q1)) {
            line = reader.readLine();
        }
        reader.readLine();
        line = reader.readLine();

        for (int q = 0; q < nqueries && !line.isEmpty(); line = reader.readLine()) {
            if (line.startsWith(".I ")) {
                if(Integer.parseInt(line.substring(3)) != q1+q+1) {
                    throw new ParseException();
                } else {
                    reader.readLine();
                    queries[q++] = parser.parse(queryLine.toString());
                }
            } else {
                queryLine.append(" ").append(parser.parse(line));
            }
        }

        return queries;
    }

    public static void main(String[] args) {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-indexin INDEX_PATH] [-search jm lambda Nº|tfidf]\n"
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
                        case "jm lambda":
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
            QueryParser parser = new QueryParser(FIELD_Q, new StandardAnalyzer());
            Query[] queries = parseQueries(parser, Path.of(FILE_Q), q1, q2);
            TopDocs topDocs;

            for (Query query: queries) {
                topDocs = indexSearcher.search(query, n);

                for (int i = 0; i < Math.min(10, topDocs.totalHits.value); i++) {
                        System.out.println(topDocs.scoreDocs[i].doc + " -- score: "
                                + topDocs.scoreDocs[i].score + " -- "
                                + indexReader.document(topDocs.scoreDocs[i].doc).get(FIELD_Q));
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
