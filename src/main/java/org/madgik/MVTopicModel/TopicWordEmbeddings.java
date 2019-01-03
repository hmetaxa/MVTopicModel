package org.madgik.MVTopicModel;

import cc.mallet.topics.TopicAssignment;
import cc.mallet.util.Randoms;
import cc.mallet.util.CommandOption;
import cc.mallet.types.*;
import java.util.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.*;
import java.nio.charset.Charset;
import org.madgik.utils.MixTopicModelTopicAssignment;

public class TopicWordEmbeddings {

    static CommandOption.String inputFile = new CommandOption.String(TopicWordEmbeddings.class, "input", "FILENAME", true, null,
            "The filename from which to read the list of training instances.  Use - for stdin.  "
            + "The instances must be FeatureSequence or FeatureSequenceWithBigrams, not FeatureVector", null);

    static CommandOption.String outputFile = new CommandOption.String(TopicWordEmbeddings.class, "output", "FILENAME", true, "weights.txt",
            "The filename to write text-formatted word vectors.", null);

    static CommandOption.String outputContextFile = new CommandOption.String(TopicWordEmbeddings.class, "output-context", "FILENAME", true, "NONE",
            "The filename to write text-formatted context vectors.", null);

    static CommandOption.Boolean outputStatsLine = new CommandOption.Boolean(TopicWordEmbeddings.class, "output-stats-prefix", "TRUE/FALSE", false, false,
            "Whether to include a line at the beginning of the output with the vocab size and vector dimension, for compatibility with other packages.", null);

    static CommandOption.Integer numDimensions = new CommandOption.Integer(TopicWordEmbeddings.class, "num-dimensions", "INTEGER", true, 50,
            "The number of dimensions to fit.", null);

    static CommandOption.Integer windowSizeOption = new CommandOption.Integer(TopicWordEmbeddings.class, "window-size", "INTEGER", true, 5,
            "The number of adjacent words to consider.", null);

    static CommandOption.Integer numThreads = new CommandOption.Integer(TopicWordEmbeddings.class, "num-threads", "INTEGER", true, 1,
            "The number of threads for parallel training.", null);

    static CommandOption.Integer numIterationsOption = new CommandOption.Integer(TopicWordEmbeddings.class, "num-iters", "INTEGER", true, 3,
            "The number of passes through the training data.", null);

    static CommandOption.Double samplingFactorOption = new CommandOption.Double(TopicWordEmbeddings.class, "frequency-factor", "NUMBER", true, 0.0001,
            "Down-sample words that account for more than ~2.5x this proportion or the corpus.", null);

    static CommandOption.Integer numSamples = new CommandOption.Integer(TopicWordEmbeddings.class, "num-samples", "INTEGER", true, 5,
            "The number of negative samples to use in training.", null);

    static CommandOption.String exampleWord = new CommandOption.String(TopicWordEmbeddings.class, "example-word", "STRING", true, null,
            "If defined, periodically show the closest vectors to this word.", null);

    static CommandOption.String orderingOption = new CommandOption.String(TopicWordEmbeddings.class, "ordering", "STRING", true, "linear",
            "\"linear\" reads documents in order, \"shuffled\" reads in random order, \"random\" selects documents at random and may repeat/drop documents", null);

    public static final int LINEAR_ORDERING = 0;
    public static final int SHUFFLED_ORDERING = 1;
    public static final int RANDOM_ORDERING = 2;

    Alphabet vocabulary;

    int numWords;
    int numTopics;
    int numVectors;
    int numColumns;
    int numContextColumns;
    double[] weights;
    double[] negativeWeights;
    int stride;

    int numIterations;

    int[] wordCounts;
    double[] retentionProbability;
    double[] samplingDistribution;
    int[] samplingTable;
    int samplingTableSize = 100000000;
    double samplingSum = 0.0;
    int totalWords = 0;

    double maxExpValue = 6.0;
    double minExpValue = -6.0;
    double[] sigmoidCache;
    int sigmoidCacheSize = 1000;

    int windowSize = 5;

    IDSorter[] sortedWords = null;

    int orderingStrategy = LINEAR_ORDERING;

    public int getMinDocumentLength() {
        return minDocumentLength;
    }

    public void setMinDocumentLength(int minDocumentLength) {
        if (minDocumentLength <= 0) {
            throw new IllegalArgumentException("Minimum document length must be at least 1.");
        }
        this.minDocumentLength = minDocumentLength;
    }

    private int minDocumentLength = 10;

    public void setNumIterations(int i) {
        numIterations = i;
    }

    public String getQueryWord() {
        return queryWord;
    }

    public void setQueryWord(String queryWord) {
        this.queryWord = queryWord;
    }

    String queryWord = "the";

    Randoms random = new Randoms();

    public TopicWordEmbeddings() {
    }

    public TopicWordEmbeddings(Alphabet a, int numColumns, int numContextColumns, int windowSize, int numOfTopics) {
        vocabulary = a;

        numWords = vocabulary.size();
        numTopics = numOfTopics;
        numVectors = numWords + numTopics;

        System.out.format("Vocab size: %d\n", numWords);

        this.numColumns = numColumns;
        this.numContextColumns = numTopics == 0 ? 0 : numContextColumns;

        this.stride = numColumns;

        weights = new double[numVectors * stride];
        negativeWeights = new double[numVectors * stride];

        for (int word = 0; word < numVectors; word++) {
            for (int col = 0; col < numColumns; col++) {
                weights[word * stride + col] = (random.nextDouble() - 0.5f) / numColumns;
                negativeWeights[word * stride + col] = 0.0;
            }
        }

        wordCounts = new int[numWords];
        samplingDistribution = new double[numWords];
        retentionProbability = new double[numWords];
        samplingTable = new int[samplingTableSize];

        this.windowSize = windowSize;

        sigmoidCache = new double[sigmoidCacheSize + 1];

        for (int i = 0; i < sigmoidCacheSize; i++) {
            double value = ((double) i / sigmoidCacheSize) * (maxExpValue - minExpValue) + minExpValue;
            sigmoidCache[i] = 1.0 / (1.0 + Math.exp(-value));
        }

    }

    public void initializeSortables() {
        sortedWords = new IDSorter[numWords];
        for (int word = 0; word < numWords; word++) {
            sortedWords[word] = new IDSorter(word, 0.0);
        }
    }

    public void countWords(InstanceList instances, double samplingFactor) {
        for (Instance instance : instances) {

            FeatureSequence tokens = (FeatureSequence) instance.getData();
            int length = tokens.getLength();

            for (int position = 0; position < length; position++) {
                int type = tokens.getIndexAtPosition(position);
                wordCounts[type]++;
            }

            totalWords += length;
        }

        for (int word = 0; word < numWords; word++) {
            // Word2Vec style sampling weight
            double frequencyScore = (double) wordCounts[word] / (samplingFactor * totalWords);
            retentionProbability[word] = Math.min((Math.sqrt(frequencyScore) + 1) / frequencyScore, 1.0);
        }

        if (sortedWords == null) {
            initializeSortables();
        }
        for (int word = 0; word < numWords; word++) {
            sortedWords[word].set(word, wordCounts[word]);
        }
        Arrays.sort(sortedWords);

        samplingDistribution[0] = Math.pow(wordCounts[sortedWords[0].getID()], 0.75);
        for (int word = 1; word < numWords; word++) {
            samplingDistribution[word] = samplingDistribution[word - 1] + Math.pow(wordCounts[sortedWords[word].getID()], 0.75);
        }
        samplingSum = samplingDistribution[numWords - 1];

        int order = 0;
        for (int i = 0; i < samplingTableSize; i++) {
            samplingTable[i] = sortedWords[order].getID();
            while (samplingSum * i / samplingTableSize > samplingDistribution[order]) {
                order++;
            }
        }

        System.out.println("done counting: " + totalWords);
    }

    public void countWords(ArrayList<MixTopicModelTopicAssignment> data, double samplingFactor) {

        int numDocuments = data.size();

        for (int docID = 0; docID < numDocuments; docID++) {

            Instance instance = data.get(docID).Assignments[0].instance;
            FeatureSequence tokens = (FeatureSequence) instance.getData();
            int length = tokens.getLength();

            /*
            int[] oneDocTopics = null;
            if (numTopics > 0) {
                LabelSequence topicSequence = (LabelSequence) data.get(docID).Assignments[0].topicSequence;
                oneDocTopics = topicSequence.getFeatures();

            }
             */
            for (int position = 0; position < length; position++) {
                int type = tokens.getIndexAtPosition(position);
                wordCounts[type]++;
                /*if (numTopics > 0) {
                    int topic = oneDocTopics[position];
                    wordCounts[numWords + topic]++;
                }*/
            }

            totalWords += length;
        }

        for (int word = 0; word < numWords; word++) {
            // Word2Vec style sampling weight
            double frequencyScore = (double) wordCounts[word] / (samplingFactor * totalWords);
            retentionProbability[word] = Math.min((Math.sqrt(frequencyScore) + 1) / frequencyScore, 1.0);
        }

        if (sortedWords == null) {
            initializeSortables();
        }
        for (int word = 0; word < numWords; word++) {
            sortedWords[word].set(word, wordCounts[word]);
        }
        Arrays.sort(sortedWords);

        samplingDistribution[0] = Math.pow(wordCounts[sortedWords[0].getID()], 0.75);
        for (int word = 1; word < numWords; word++) {
            samplingDistribution[word] = samplingDistribution[word - 1] + Math.pow(wordCounts[sortedWords[word].getID()], 0.75);
        }
        samplingSum = samplingDistribution[numWords - 1];

        int order = 0;
        for (int i = 0; i < samplingTableSize; i++) {
            samplingTable[i] = sortedWords[order].getID();
            while (samplingSum * i / samplingTableSize > samplingDistribution[order]) {
                order++;
            }
        }

        System.out.println("done counting: " + totalWords);
    }

    public void train(InstanceList instances, int numThreads, int numSamples, int numOfIterations) {

        ArrayList<MixTopicModelTopicAssignment> data = new ArrayList<MixTopicModelTopicAssignment>();

        for (Instance instance : instances) {
            TopicAssignment t = new TopicAssignment(instance, null);

            //data.add(t);
            MixTopicModelTopicAssignment mt;
            String entityId = (String) instance.getName();
            mt = new MixTopicModelTopicAssignment(entityId, new TopicAssignment[1]);
            mt.Assignments[0] = t;
            data.add(mt);

        }

        train(data, numThreads, numSamples, numOfIterations);
    }

    public void train(ArrayList<MixTopicModelTopicAssignment> data, int numThreads, int numSamples, int numOfIterations) {

        numIterations = numOfIterations;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        TopicWordEmbeddingRunnable[] runnables = new TopicWordEmbeddingRunnable[numThreads];
        for (int thread = 0; thread < numThreads; thread++) {
            runnables[thread] = new TopicWordEmbeddingRunnable(this, data, numSamples, numThreads, thread, numWords, numTopics);
            runnables[thread].setOrdering(orderingStrategy);
            executor.submit(runnables[thread]);
        }

        long startTime = System.currentTimeMillis();
        double difference = 0.0;

        boolean finished = false;
        while (!finished) {

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {

            }

            long wordsSoFar = 0;

            // Are all the threads done?
            boolean anyRunning = false;
            double sumMeanError = 0.0;
            for (int thread = 0; thread < numThreads; thread++) {
                if (runnables[thread].shouldRun) {
                    anyRunning = true;
                }
                wordsSoFar += runnables[thread].wordsSoFar;
                sumMeanError += runnables[thread].getMeanError();
                //System.out.format("%.3f ", runnables[thread].getMeanError());
            }

            long runningMillis = System.currentTimeMillis() - startTime;
            System.out.format("%d\t%d\t%fk w/s %.3f avg %.3f step\n", wordsSoFar, runningMillis, (double) wordsSoFar / runningMillis,
                    averageAbsWeight(), sumMeanError / numThreads);
            //variances();
            difference = 0.0;

            if (!anyRunning || (long) wordsSoFar > (long) numIterations * (long) totalWords) {
                finished = true;
                for (int thread = 0; thread < numThreads; thread++) {
                    runnables[thread].shouldRun = false;
                }
            }

            if (queryWord != null && vocabulary.contains(queryWord)) {
                findClosest(copy(queryWord));
            }
        }
        executor.shutdownNow();
    }

    public void findClosest(double[] targetVector) {

        double targetSquaredSum = 0.0;
        for (int col = 0; col < numColumns; col++) {
            targetSquaredSum += targetVector[col] * targetVector[col];
        }
        double targetNormalizer = 1.0 / Math.sqrt(targetSquaredSum);

        IDSorter[] sortedWords = new IDSorter[numWords];
        for (int word = 0; word < numWords; word++) {
            sortedWords[word] = new IDSorter(word, 0.0);
        }

        for (int word = 0; word < numWords; word++) { //look only in word vectors  --> numVectors-numTopics

            double innerProduct = 0.0;

            double wordSquaredSum = 0.0;
            for (int col = 0; col < numColumns; col++) {
                wordSquaredSum += weights[word * stride + col] * weights[word * stride + col];
            }
            double wordNormalizer = 1.0 / Math.sqrt(wordSquaredSum);

            for (int col = 0; col < numColumns; col++) {
                innerProduct += targetVector[col] * weights[word * stride + col];
            }
            innerProduct *= targetNormalizer * wordNormalizer;

            sortedWords[word].set(word, innerProduct);
        }

        Arrays.sort(sortedWords);

        for (int i = 0; i < 10; i++) {
            System.out.format("Similar Word: %f\t%d\t%s\n", sortedWords[i].getWeight(), sortedWords[i].getID(), vocabulary.lookupObject(sortedWords[i].getID()));
        }

        if (numTopics > 0) {
            IDSorter[] sortedTopics = new IDSorter[numTopics];
            for (int topic = 0; topic < numTopics; topic++) {
                sortedTopics[topic] = new IDSorter(topic, 0.0);
            }

            for (int topic = 0; topic < numTopics; topic++) { //look only in topics vectors  

                double innerProduct = 0.0;

                double wordSquaredSum = 0.0;
                for (int col = 0; col < numColumns; col++) {
                    wordSquaredSum += weights[(numWords + topic) * stride + col] * weights[(numWords + topic) * stride + col];
                }
                double wordNormalizer = 1.0 / Math.sqrt(wordSquaredSum);

                for (int col = 0; col < numColumns; col++) {
                    innerProduct += targetVector[col] * weights[(numWords + topic) * stride + col];
                }
                innerProduct *= targetNormalizer * wordNormalizer;

                sortedTopics[topic].set(topic, innerProduct);
            }

            Arrays.sort(sortedTopics);

            for (int i = 0; i < 10; i++) {
                System.out.format("Similar Topic: %f\t%d\n", sortedTopics[i].getWeight(), sortedTopics[i].getID());
            }
        }
    }

    public double averageAbsWeight() {
        double sum = 0.0;
        for (int word = 0; word < numVectors; word++) {
            for (int col = 0; col < numColumns; col++) {
                sum += Math.abs(weights[word * stride + col]);
            }
        }
        return sum / (numVectors * numColumns);
    }

    public double[] variances() {
        double[] means = new double[numColumns];
        for (int word = 0; word < numVectors; word++) {
            for (int col = 0; col < numColumns; col++) {
                means[col] += weights[word * stride + col];
            }
        }
        for (int col = 0; col < numColumns; col++) {
            means[col] /= numVectors;
        }

        double[] squaredSums = new double[numColumns];
        double diff;
        for (int word = 0; word < numVectors; word++) {
            for (int col = 0; col < numColumns; col++) {
                diff = weights[word * stride + col] - means[col];
                squaredSums[col] += diff * diff;
            }
        }
        for (int col = 0; col < numColumns; col++) {
            squaredSums[col] /= (numVectors - 1);
            System.out.format("%f\t", squaredSums[col]);
        }
        System.out.println();
        return squaredSums;
    }

    public void write(PrintWriter out) {
        for (int word = 0; word < numVectors; word++) {
            Formatter buffer = new Formatter(Locale.US);
            buffer.format("%s", word < numWords ? vocabulary.lookupObject(word).toString() : String.valueOf(word - numWords));
            for (int col = 0; col < numColumns; col++) {
                buffer.format(" %.6f", weights[word * stride + col]);
            }
            out.println(buffer);
        }
    }

    public void write(String SQLLiteDB, int modality) {
        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();
                statement.executeUpdate("drop table if exists wordvector");
                statement.executeUpdate("create table if not exists wordvector (word text, columnid integer, weight numeric, modality integer ) ");
                //statement.executeUpdate(String.format("Delete from PubTopic where  ExperimentId = '%s'", experimentId));
            }
            PreparedStatement bulkInsert = null;
            String sql = "insert into wordvector values(?,?,?,? );";

            try {
                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (int word = 0; word < numVectors; word++) {
                    //Formatter buffer = new Formatter(new StringBuilder(), Locale.US);

                    //buffer.format("%s", vocabulary.lookupObject(word));
                    for (int col = 0; col < numColumns; col++) {
                        //   buffer.format(" %.6f \t", weights[word * stride + col]);

                        bulkInsert.setString(1, word < numWords ? vocabulary.lookupObject(word).toString() : String.valueOf(word - numWords));
                        bulkInsert.setInt(2, col);
                        bulkInsert.setDouble(3, weights[word * stride + col]);
                        bulkInsert.setInt(4, word < numWords ? modality : -1);
                        bulkInsert.executeUpdate();

                    }
                }

                if (!SQLLiteDB.isEmpty()) {
                    connection.commit();
                }

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert topicAnalysis");
                    }
                }
            } finally {

                if (bulkInsert != null) {
                    bulkInsert.close();
                }
                connection.setAutoCommit(true);
            }

        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            System.err.println(e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                System.err.println(e);
            }

        }
    }

    public void writeContext(PrintWriter out) {
        for (int word = 0; word < numVectors; word++) {
            Formatter buffer = new Formatter(Locale.US);
            buffer.format("%s", word < numWords ? vocabulary.lookupObject(word).toString() : String.valueOf(word - numWords));
            for (int col = 0; col < numColumns; col++) {
                buffer.format(" %.6f", negativeWeights[word * stride + col]);
            }
            out.println(buffer);
        }
    }

    public double[] copy(String word) {
        return copy(vocabulary.lookupIndex(word));
    }

    public double[] copy(int word) {
        double[] result = new double[numColumns];

        for (int col = 0; col < numColumns; col++) {
            result[col] = weights[word * stride + col];
        }

        return result;
    }

    public double[] add(double[] result, String word) {
        return add(result, vocabulary.lookupIndex(word));
    }

    public double[] add(double[] result, int word) {
        for (int col = 0; col < numColumns; col++) {
            result[col] += weights[word * stride + col];
        }

        return result;
    }

    public double[] subtract(double[] result, String word) {
        return subtract(result, vocabulary.lookupIndex(word));
    }

    public double[] subtract(double[] result, int word) {
        for (int col = 0; col < numColumns; col++) {
            result[col] -= weights[word * stride + col];
        }

        return result;
    }

    public double[][] getWordVectors() {
        double[][] result = new double[numWords][numColumns];
        for (int word = 0; word < numWords; word++) {
            for (int col = 0; col < numColumns; col++) {
                result[word][col] = weights[word * stride + col];
            }
        }

        return result;
    }

    public double[][] getTopicVectors() {
        double[][] result = new double[numTopics][numColumns];
        for (int topic = 0; topic < numTopics; topic++) {
            for (int col = 0; col < numColumns; col++) {
                result[topic][col] = weights[(numWords + topic) * stride + col];
            }
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        // Process the command-line options
        CommandOption.setSummary(TopicWordEmbeddings.class,
                "Train continuous word embeddings using the skip-gram method with negative sampling.");
        CommandOption.process(TopicWordEmbeddings.class, args);

        InstanceList instances = InstanceList.load(new File(inputFile.value));

        TopicWordEmbeddings matrix = new TopicWordEmbeddings(instances.getDataAlphabet(), numDimensions.value, 0, windowSizeOption.value, 0);
        matrix.queryWord = exampleWord.value;
        //matrix.setNumIterations(numIterationsOption.value);
        matrix.countWords(instances, samplingFactorOption.value);
        if (orderingOption.value != null) {
            if (orderingOption.value.startsWith("s")) {
                matrix.orderingStrategy = SHUFFLED_ORDERING;
            } else if (orderingOption.value.startsWith("l")) {
                matrix.orderingStrategy = LINEAR_ORDERING;
            } else if (orderingOption.value.startsWith("r")) {
                matrix.orderingStrategy = RANDOM_ORDERING;
            } else {
                System.err.println("Unrecognized ordering: " + orderingOption.value + ", using linear.");
            }
        }

        matrix.train(instances, numThreads.value, numSamples.value, numIterationsOption.value);

        PrintWriter out = new PrintWriter(outputFile.value, Charset.defaultCharset().name());
        if (outputStatsLine.value) {
            out.write(matrix.numWords + " " + matrix.numColumns + "\n");
        }
        matrix.write(out);
        out.close();

        if (outputContextFile.value != null) {
            out = new PrintWriter(outputContextFile.value, Charset.defaultCharset().name());
            if (outputStatsLine.value) {
                out.write(matrix.numWords + " " + matrix.numColumns + "\n");
            }
            matrix.writeContext(out);
            out.close();
        }
    }

}
