package es.udc.fi.ri.nicoedu;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexMedline implements AutoCloseable {

    static final String KNN_DICT = "knn-dict";

    public static final FieldType TYPE_STORED = new FieldType();

    static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

    static {
        TYPE_STORED.setIndexOptions(options);
        TYPE_STORED.setTokenized(true);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setStoreTermVectors(true);
        TYPE_STORED.setStoreTermVectorPositions(true);
        TYPE_STORED.freeze();
    }

    // Calculates embedding vectors for KnnVector search
    private final DemoEmbeddings demoEmbeddings;
    private final KnnVectorDict vectorDict;
    private final Properties fileProp = new Properties();

    public static class WorkerThread implements Runnable {
        private final IndexWriter writer;
        private final IndexMedline indexMedline;
        private final Path folder;
        private final int maxDepth;

        public WorkerThread(final IndexWriter writer,
                            final IndexMedline indexFiles,
                            final Path folder,
                            final int maxDepth) {
            this.writer = writer;
            this.indexMedline = indexFiles;
            this.folder = folder;
            this.maxDepth = maxDepth;
        }

        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {
            try {
                indexMedline.indexDocs(writer, folder, maxDepth);
            } catch (final IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                    Thread.currentThread().getName(), folder));
        }
    }

    private IndexMedline(KnnVectorDict vectorDict) throws IOException {
        fileProp.load(Files.newInputStream(
                Path.of("src/main/resources/config.properties")));
        if (vectorDict != null) {
            this.vectorDict = vectorDict;
            demoEmbeddings = new DemoEmbeddings(vectorDict);
        } else {
            this.vectorDict = null;
            demoEmbeddings = null;
        }
    }

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH] \n"
                + "[-openmode append|create|create_append] [-numThreads Nº] [-deep Nº] \n"
                + "[-indexingmodel jm lambda Nº|tfidf]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles\n"
                + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexPath = "index";
        String docsPath = null;
        String vectorDictSource = null;
        IndexWriterConfig.OpenMode openMode = IndexWriterConfig.OpenMode.CREATE;
        int numThreads = Runtime.getRuntime().availableProcessors();
        boolean partialIndexes = false;
        int maxDepth = Integer.MAX_VALUE;
        Similarity model = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
                    break;
                case "-update":
                    openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
                    break;
                case "-create":
                    openMode = IndexWriterConfig.OpenMode.CREATE;
                    break;
                case "-openmode":
                    switch (args[++i]) {
                        case "append":
                            openMode = IndexWriterConfig.OpenMode.APPEND;
                            break;
                        case "create":
                            openMode = IndexWriterConfig.OpenMode.CREATE;
                            break;
                        case "create_or_append":
                            openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
                            break;
                        default:
                            throw new IllegalArgumentException("unknown parameter " + args[i]);
                    }
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-partialIndexes":
                    partialIndexes = true;
                    break;
                case "-deep":
                    maxDepth = Integer.parseInt(args[++i]);
                    break;
                case "-indexingmodel":
                    switch (args[++i]) {
                        case "jm":
                            model = new LMJelinekMercerSimilarity(Float.parseFloat(args[++i]));
                            break;
                        case "tfidf":
                            model = new ClassicSimilarity();
                            break;
                        default:
                            throw new IllegalArgumentException("unknown parameter " + args[i]);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null || model == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Path indexDir = Paths.get(indexPath);
            Directory dir = FSDirectory.open(indexDir);
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer).setSimilarity(model);
            iwc.setOpenMode(openMode);

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer. But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            KnnVectorDict vectorDictInstance = null;
            long vectorDictSize = 0;
            if (vectorDictSource != null) {
                KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
                vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
                vectorDictSize = vectorDictInstance.ramBytesUsed();
            }

            final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            final List<IndexWriter> subwriters = new ArrayList<>();
            IndexWriter subwriter;

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 IndexMedline indexMedline = new IndexMedline(vectorDictInstance);
                 DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {

                for (final Path path : directoryStream) {
                    if (Files.isDirectory(path)) {
                        if(partialIndexes) {
                            Path subIndexDir = Path.of(indexDir.getParent().toString() + "/" +
                                    path.toFile().getName());

                            if(!Files.exists(subIndexDir)) {
                                if(openMode == IndexWriterConfig.OpenMode.CREATE ||
                                        openMode == IndexWriterConfig.OpenMode.CREATE_OR_APPEND) {
                                    Files.createDirectory(subIndexDir);
                                } else {
                                    throw new IOException();
                                }
                            }
                            Directory subdir = FSDirectory.open(subIndexDir);

                            subwriter = new IndexWriter(subdir,
                                    new IndexWriterConfig(analyzer)
                                            .setOpenMode(openMode)
                                            .setSimilarity(model));
                            subwriters.add(subwriter);
                        } else {
                            subwriter = writer;
                        }

                        executor.execute(new WorkerThread(subwriter, indexMedline, path, maxDepth));
                    }
                }

                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.HOURS);

                if(partialIndexes) {
                    int dirs = subwriters.size();
                    IndexWriter subw;
                    Directory[] subdirectories = new Directory[dirs];

                    for (int i = 0; i < dirs; i++) {
                        subw = subwriters.get(i);
                        subdirectories[i] = subw.getDirectory();
                        subw.close();
                    }
                    writer.addIndexes(subdirectories);
                }

            } catch (final IOException e) {
                e.printStackTrace();
                System.exit(-1);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                System.exit(-2);
            } finally {
                IOUtils.close(vectorDictInstance);
            }

            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                        + " milliseconds");
				/*if (reader.numDocs() > 100 && vectorDictSize < 1_000_000 && System.getProperty("smoketester") == null) {
					throw new RuntimeException(
							"Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
				}*/
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is given,
     * recurses over files and directories found under the given directory.
     *
     * <p>
     * NOTE: This method indexes one document per input file. This is slow. For good
     * throughput, put multiple documents into your input file(s). An example of
     * this is in the benchmark module, which can create "line doc" files, one
     * document per line, using the <a href=
     * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be
     *               stored
     * @param path   The file to index, or the directory to recurse into to find
     *               files to indt
     * @throws IOException If there is a low-level I/O error
     */
    void indexDocs(final IndexWriter writer, Path path, int maxDepth) throws IOException {
        String aux = this.fileProp.getProperty("onlyFiles");
        String[] onlyFiles = aux == null? null: aux.split(" ");

        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), maxDepth,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            try {
                                if(onlyFiles == null ||
                                        Arrays.stream(onlyFiles).anyMatch((ext) -> file.toString().endsWith(ext))) {
                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                                }
                            } catch (@SuppressWarnings("unused") IOException ignore) {
                                ignore.printStackTrace(System.err);
                                // don't index files that can't be read.
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    /** Indexes a single document */
    void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {

            // make a new, empty document
            Document doc = new Document();

            doc.add(new LongPoint("docIDMedline", Long.parseLong(file.toFile().getName())));

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            doc.add(new TextField("contents", reader));

            if (demoEmbeddings != null) {
                try (InputStream in = Files.newInputStream(file)) {
                    float[] vector = demoEmbeddings
                            .computeEmbedding(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
                    doc.add(new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
                }
            }

            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorDict);
    }
}