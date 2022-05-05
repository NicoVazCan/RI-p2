package es.udc.fi.ri.nicoedu;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.DoubleStream;

public class TrainingTestMedline {

    public static void main(String[] args) {
        String usage = "Usage: TrainingTestMedline [-indexin pathname] [-evaljm int1-int2 int3-int4] [-cut n]" +
                "[-metrica P | R | MAP]";
        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))){
            System.out.println(usage);
            System.exit(0);
        }
        String docsPath = null;
        String index = null;
        int cut = 5;
        String metrica = null;
        boolean jm = false;
        String queriesFile = null, relDocsFile = null, fileName = null;
        Integer startTrain = null, endTrain = null, startTest = null, endTest = null;

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
                String[] qTrain = args[i+1].split("-"),
                        qTest = args[i+2].split("-");

                if (qTrain.length == 2 && qTest.length == 2){
                    startTrain = Integer.parseInt(qTrain[0]);
                    endTrain = Integer.parseInt(qTrain[1]);
                    startTest = Integer.parseInt(qTest[0]);
                    endTest = Integer.parseInt(qTest[1]);
                } else {
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
                }
                i += 2;
                jm = true;
            } else if ("-evaltfidf".equals(args[i])) {
                String[] qTest = args[i+1].split("-");

                if (qTest.length == 2){
                    startTrain = 0;
                    endTrain = 0;
                    startTest = Integer.parseInt(qTest[0]);
                    endTest = Integer.parseInt(qTest[1]);
                } else {
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
                }
                i++;
            } else if ("-outputdir".equals(args[i])) {
                docsPath = args[i+1];
                i++;
            } else if ("-queriesfile".equals(args[i])) {
                queriesFile = args[i+1];
                i++;
            } else if ("-reldocsfile".equals(args[i])) {
                relDocsFile = args[i+1];
                i++;
            }
        }

        if (index == null || metrica == null ||
                (!metrica.equals("R") && !metrica.equals("P") && !metrica.equals("MAP")) ||
                startTrain == null || endTrain == null || startTest == null || endTest == null ||
                queriesFile == null || relDocsFile == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        System.out.println("Searching " + index + ", with: " + metrica + " metrica\n");

        if (docsPath != null) {
            fileName = "medline." + (jm? "jm": "tfidf") + ".training."
                    + (jm? startTrain+"-"+endTrain: "null") + ".test."
                    + startTest + "-" + endTest + "." +
                    metrica.toLowerCase() + cut;
        }

        try (Directory indexDir = FSDirectory.open(Path.of(index));
             IndexReader reader = DirectoryReader.open(indexDir);
             PrintStream trainOutput = docsPath != null? new PrintStream(Files.newOutputStream(
                     Path.of(docsPath+fileName+".train.csv"))): null;
             PrintStream testOutput = docsPath != null? new PrintStream(Files.newOutputStream(
                     Path.of(docsPath+fileName+".test.csv"))): null) {

            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new ClassicSimilarity());
            QueryParser parser = new QueryParser(SearchEvalMedline.FIELD_C, new StandardAnalyzer());

            Query[] trainQueries = SearchEvalMedline.parseQueries(parser, Path.of(queriesFile), startTrain, endTrain);
            Query[] testQueries = SearchEvalMedline.parseQueries(parser, Path.of(queriesFile), startTest, endTest);
            List<String>[] trainQueriesRelDocs = SearchEvalMedline.parseRelDocs(Path.of(relDocsFile), startTrain, endTrain);
            List<String>[] testQueriesRelDocs = SearchEvalMedline.parseRelDocs(Path.of(relDocsFile), startTest, endTest);
            TopDocs topDocs;

            int nRel = 0, APAn = 0, nqueriesRel = 0;
            double medMet, maxMedMet = 0;
            double[] mets = new double[trainQueries.length];
            float lambda = 0f;
            double[][] csvData = new double[9][trainQueries.length];
            int l = 0;

            System.out.println("TRAINING:\n");
            if (trainOutput != null) {
                trainOutput.print(metrica+"@"+cut);
            }

            for (float p = 0.1f; p <= 1; p+=0.1, l++) {
                System.out.println("Queries con lambda = "+p+":");
                searcher.setSimilarity(new LMJelinekMercerSimilarity(p));
                if (trainOutput != null) {
                    trainOutput.print(',');
                    trainOutput.print(p);
                }

                for (int q = 0; q < trainQueries.length; q++) {
                    topDocs = searcher.search(trainQueries[q], cut);

                    System.out.println("Query: " + (q+startTrain) + "\n"
                                    + trainQueries[q].toString().replaceAll("contents:", "| ") + "\n");

                    for (int i = 0; i < Math.min(cut, topDocs.totalHits.value); i++) {
                        String da = reader.document(topDocs.scoreDocs[i].doc).get("docIDMedline");

                        System.out.println("idLucene: " + topDocs.scoreDocs[i].doc + ", score: "
                                        + topDocs.scoreDocs[i].score + ", idMedline: "
                                        + reader.document(topDocs.scoreDocs[i].doc).get("docIDMedline"));

                        if (trainQueriesRelDocs[q].stream().anyMatch(da::equals)) {
                            nRel++;
                            APAn += nRel/(i+1);
                        }
                    }
                    if (nRel > 0) {
                        nqueriesRel++;
                    }
                    System.out.println();

                    mets[q] = metrica.equals("P")? ((double) nRel)/cut:
                        metrica.equals("R")? ((double) nRel)/trainQueriesRelDocs[q].size():
                        metrica.equals("MAP")? ((double) APAn)/trainQueriesRelDocs[q].size() : 0;

                    System.out.println(metrica+"@"+cut+": " + mets[q] + "\n\n");
                    csvData[l][q] = mets[q];
                }

                medMet = DoubleStream.of(mets).sum()/nqueriesRel;

                System.out.println("media de "+metrica+"@"+cut+": " + medMet + "\n\n\n");

                if (maxMedMet < medMet) {
                    maxMedMet = medMet;
                    lambda = p;
                }
            }
            if (trainOutput != null) {
                trainOutput.println();
            }

            nRel = 0; APAn = 0; nqueriesRel = 0;
            mets = new double[testQueries.length];
            if (jm) {
                System.out.println("\nMejor lamda: " + lambda+ ", con una meta de: "+maxMedMet);
                searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
            }

            for (int q = 0; q < trainQueries.length && trainOutput != null; q++) {
                trainOutput.print(q+startTrain);
                for (l = 0; l < 8; l++) {
                    trainOutput.print(',');
                    trainOutput.print(csvData[l][q]);
                }
                trainOutput.print(',');
                trainOutput.print(csvData[l][q]);
                trainOutput.println();
            }

            if (trainOutput != null) {
                trainOutput.print("Media");
                for (l = 0; l < 8; l++) {
                    trainOutput.print(',');
                    trainOutput.print(DoubleStream.of(csvData[l]).average().getAsDouble());
                }
                trainOutput.print(',');
                trainOutput.print(DoubleStream.of(csvData[l]).average().getAsDouble());
                trainOutput.println();
            }

            System.out.println("TESTING:\n");
            if (testOutput != null) {
                testOutput.println(lambda+","+metrica+"@"+cut);
            }

            for (int q = 0; q < testQueries.length; q++) {
                topDocs = searcher.search(testQueries[q], cut);

                System.out.println("Query: " + (q+startTrain) + "\n"
                        + testQueries[q].toString().replaceAll("contents:", "| ") + "\n");

                for (int i = 0; i < Math.min(cut, topDocs.totalHits.value); i++) {
                    String da = reader.document(topDocs.scoreDocs[i].doc).get("docIDMedline");

                    System.out.println("idLucene: " + topDocs.scoreDocs[i].doc + ", score: "
                            + topDocs.scoreDocs[i].score + ", idMedline: "
                            + reader.document(topDocs.scoreDocs[i].doc).get("docIDMedline"));

                    if (testQueriesRelDocs[q].stream().anyMatch(da::equals)) {
                        nRel++;
                        APAn += nRel/(i+1);
                    }
                }
                if (nRel > 0) {
                    nqueriesRel++;
                }
                System.out.println();

                mets[q] = metrica.equals("P")? ((double) nRel)/cut:
                        metrica.equals("R")? ((double) nRel)/testQueriesRelDocs[q].size():
                                metrica.equals("MAP")? ((double) APAn)/testQueriesRelDocs[q].size() : 0;

                System.out.println(metrica+"@"+cut+": " + mets[q] + "\n\n");

                if (testOutput != null) {
                    testOutput.println(q+startTest+","+mets[q]);
                }
            }

            medMet = DoubleStream.of(mets).sum()/nqueriesRel;
            System.out.println("M"+metrica+"@"+cut+": " + medMet + "\n\n\n");
            if (testOutput != null) {
                testOutput.println("Media,"+medMet);
            }


        } catch (IOException | org.apache.lucene.queryparser.classic.ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
