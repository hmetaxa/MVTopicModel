package org.madgik.MVTopicModel;

import org.madgik.utils.FastQDelta;
import org.madgik.utils.MixTopicModelTopicAssignment;
import org.madgik.utils.FTree;
import org.madgik.utils.Utils;
import cc.mallet.topics.MarginalProbEstimator;
import cc.mallet.topics.TopicAssignment;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Formatter;
import java.util.Locale;

import java.util.concurrent.*;
import java.util.logging.*;
import java.util.zip.*;

import java.io.*;
import java.text.NumberFormat;

import cc.mallet.types.*;
import static cc.mallet.types.MatrixOps.dotProduct;
import cc.mallet.util.ArrayUtils;
import cc.mallet.util.Randoms;
import org.apache.log4j.Logger;
import gnu.trove.list.array.TByteArrayList;
//import gnu.trove.TByteArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import static java.lang.Math.log;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import org.knowceans.util.RandomSamplers;
import org.knowceans.util.Samplers;
import org.knowceans.util.Vectors;

/**
 * Parallel multi-view topic model using FTrees & queues.
 *
 * @author Omiros Metaxas
 */
public class FastQMVWVParallelTopicModel implements Serializable {

    public static final int UNASSIGNED_TOPIC = -1;

    public static Logger logger = Logger.getLogger(PTMFlow.class.getName());

    public ArrayList<MixTopicModelTopicAssignment> data;  // the training instances and their topic assignments
    public Alphabet[] alphabet; // the alphabet for the input data
    public LabelAlphabet topicAlphabet;  // the alphabet for the topics
    public byte numModalities; // Number of modalities
    public int numTopics; // Number of topics to be fit

    // These values are used to encode type/topic counts as
    //  count/topic pairs in a single int.
    //public int topicMask;
    //public int topicBits;
    public int[] numTypes; // per modality
    public int[] totalTokens; //per modality
    public int[] totalDocsPerModality; //number of docs containing this modality 

    public double[][] alpha;	 // Dirichlet(alpha,alpha,...) is the distribution over topics
    public double[] alphaSum;
    public double[] beta;   // Prior on per-topic multinomial distribution over words
    public double[] betaSum;
    public double[] gamma;

    public double[] docSmoothingOnlyMass;
    public double[][] docSmoothingOnlyCumValues;

    public List<Integer> inActiveTopicIndex = new LinkedList<Integer>(); //inactive topic index for all modalities

    public boolean usingSymmetricAlpha = false;

    public static final double DEFAULT_BETA = 0.01;

    //we should only have one updating thread that updates global counts, 
    // otherwise use AtomicIntegerArray for tokensPerTopic and split typeTopicCounts in such a way that only one thread updates topicCounts for a specific type
    public int[][][] typeTopicCounts; //
    public int[][] tokensPerTopic; // indexed by <topic index> 
    public FTree[][] trees; //store 
    public double[][] typeDiscrWeight;
    //public FTree betaSmoothingTree; //store  we will have big overhead on updating (two more tree updates)

    //public List<ConcurrentLinkedQueue<FastQDelta>> queues;
    // for dirichlet estimation
    public int[][] docLengthCounts; // histogram of document sizes
    public int[][][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>
    private int[] histogramSize;//= 0;

    public int numIterations = 1000;
    public int burninPeriod = 200;
    public int saveSampleInterval = 10;
    public int optimizeInterval = 50;
    //public int temperingInterval = 0;

    public int showTopicsInterval = 50;
    public int wordsPerTopic = 15;

    public int saveStateInterval = 0;
    public String stateFilename = null;

    public int saveModelInterval = 0;
    public String modelFilename = null;

    public int randomSeed = -1;
    public NumberFormat formatter;
    public boolean printLogLikelihood = true;

    public double[][] p_a; // a for beta prior for modalities correlation
    public double[][] p_b; // b for beta prir for modalities correlation
    //public double[][][] pDistr_Mean; // modalities correlation distribution accross documents (used in a, b beta params optimization)
    //public double[][][] pDistr_Var; // modalities correlation distribution accross documents (used in a, b beta params optimization)
    public double[][] pMean; // modalities correlation

    protected double[] tablesCnt; // tables count per modality 
    protected double rootTablesCnt; // root tables count
    protected double gammaView[]; // gammaRoot for all modalities (sumTables cnt)
    protected double gammaRoot = 10; // gammaRoot for all modalities (sumTables cnt)

    protected RandomSamplers samp;
    protected Randoms random;

    public double[][] perplexities;//= new TObjectIntHashMap<Double>(); 
    public StringBuilder expMetadata = new StringBuilder(1000);

    public boolean useCycleProposals = true;

    public String batchId = "";

    public double[][] typeVectors; // Vector representations for tokens -txt only- <token, vector>
    public double[][] topicVectors;// Vector representations for topics < topic, vector>

    public int vectorSize = 200; // Number of vector dimensions per modality
    //public int[][][] typeTopicSimilarity; //<token, topic, topic>; * 10.000 (similarity: [0,1] * 10000)

    public boolean useTypeVectors;
    public boolean trainTypeVectors;
    public double useVectorsLambda = 0;
    public double vectorsLambda = 0.6;
    public double[][] expDotProductValues;  //<topic,token>
    public double[] sumExpValues; // Partition function values per topic 

    public String SQLLiteDB;

    // The number of times each type appears in the corpus
    int[][] typeTotals;
    // The max over typeTotals, used for beta[0] optimization
    int[] maxTypeCount;

    int numThreads = 1;

    private static LabelAlphabet newLabelAlphabet(int numTopics) {
        LabelAlphabet ret = new LabelAlphabet();
        for (int i = 0; i < numTopics; i++) {
            ret.lookupIndex("topic" + i);
        }
        return ret;
    }

    public FastQMVWVParallelTopicModel(int numberOfTopics, byte numModalities, double alpha, double beta, boolean useCycleProposals, String SQLLiteDB, boolean useTypeVectors, double vectorsLambda, boolean trainTypeVectors) {

        this.SQLLiteDB = SQLLiteDB;
        this.useTypeVectors = useTypeVectors;
        this.vectorsLambda = vectorsLambda;
        this.trainTypeVectors = trainTypeVectors;
        this.numModalities = numModalities;
        this.useCycleProposals = useCycleProposals;
        this.data = new ArrayList<MixTopicModelTopicAssignment>();
        this.topicAlphabet = newLabelAlphabet(numberOfTopics);
        this.numTopics = numberOfTopics;

        this.alphaSum = new double[numModalities];
        this.alpha = new double[numModalities][numTopics + 1]; //in order to include new topic probability
        this.totalTokens = new int[numModalities];
        this.betaSum = new double[numModalities];
        this.beta = new double[numModalities];
        this.gamma = new double[numModalities];

        this.docSmoothingOnlyMass = new double[numModalities];
        this.docSmoothingOnlyCumValues = new double[numModalities][numTopics];

        tokensPerTopic = new int[numModalities][numTopics];

        for (Byte m = 0; m < numModalities; m++) {
            this.alphaSum[m] = numTopics * alpha;
            Arrays.fill(this.alpha[m], alpha);
            this.beta[m] = beta;
            this.gamma[m] = 1;
        }

        this.alphabet = new Alphabet[numModalities];
        this.totalDocsPerModality = new int[numModalities];

        formatter = NumberFormat.getInstance();
        formatter.setMaximumFractionDigits(5);

        logger.info("FastQMV LDANumTopics: " + numTopics + ", Modalities: " + this.numModalities + ", Iterations: " + this.numIterations);

        appendMetadata("Initial NumTopics: " + numTopics + ", Modalities: " + this.numModalities + ", Iterations: " + this.numIterations);
        p_a = new double[numModalities][numModalities];
        p_b = new double[numModalities][numModalities];
        pMean = new double[numModalities][numModalities];; // modalities correlation

        this.numTypes = new int[numModalities];

        perplexities = new double[numModalities][200];

        this.samp = new RandomSamplers(ThreadLocalRandom.current());

        tablesCnt = new double[numModalities];
        gammaView = new double[numModalities];

        random = null;
        if (randomSeed == -1) {
            random = new Randoms();
        } else {
            random = new Randoms(randomSeed);
        }
    }

    public StringBuilder getExpMetadata() {
        return expMetadata;
    }

    private void appendMetadata(String line) {
        expMetadata.append(line + "\n");
    }

    public Alphabet[] getAlphabet() {
        return alphabet;
    }

    public LabelAlphabet getTopicAlphabet() {
        return topicAlphabet;
    }

    public int getNumTopics() {
        return numTopics;
    }

    public ArrayList<MixTopicModelTopicAssignment> getData() {
        return data;
    }

    public void setNumIterations(int numIterations) {
        this.numIterations = numIterations;
    }

    public void setBurninPeriod(int burninPeriod) {
        this.burninPeriod = burninPeriod;
    }

    public void setTopicDisplay(int interval, int n) {
        this.showTopicsInterval = interval;
        this.wordsPerTopic = n;
    }

    public void setRandomSeed(int seed) {
        randomSeed = seed;
    }

    /**
     * Interval for optimizing Dirichlet hyperparameters
     */
    public void setOptimizeInterval(int interval) {
        this.optimizeInterval = interval;

        // Make sure we always have at least one sample
        //  before optimizing hyperparameters
        if (saveSampleInterval > optimizeInterval) {
            saveSampleInterval = optimizeInterval;
        }
    }

    public void setSymmetricAlpha(boolean b) {
        usingSymmetricAlpha = b;
    }

    public void setNumThreads(int threads) {
        this.numThreads = threads;
    }

    /**
     * Define how often and where to save a text representation of the current
     * state. Files are GZipped.
     *
     * @param interval Save a copy of the state every <code>interval</code>
     * iterations.
     * @param filename Save the state to this file, with the iteration number as
     * a suffix
     */
    public void setSaveState(int interval, String filename) {
        this.saveStateInterval = interval;
        this.stateFilename = filename;
    }

    /**
     * Define how often and where to save a serialized model.
     *
     * @param interval Save a serialized model every <code>interval</code>
     * iterations.
     * @param filename Save to this file, with the iteration number as a suffix
     */
    public void setSaveSerializedModel(int interval, String filename) {
        this.saveModelInterval = interval;
        this.modelFilename = filename;
    }

    public void readWordVectorsFile(String pathToWordVectorsFile, Alphabet[] alphabet) //word vectors for text modality
            throws Exception {
        System.out.println("Reading word vectors from word-vectors file " + pathToWordVectorsFile
                + "...");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(pathToWordVectorsFile));
            String[] elements = br.readLine().trim().split("\\s+");
            this.vectorSize = elements.length - 1;
            this.topicVectors = new double[numTopics][vectorSize];// Vector representations for topics <topic, vector>

            //this.typeVectors = new double[][]; // Vector representations for tokens per modality <modality, token, vector>
            this.typeVectors = new double[alphabet[0].size()][vectorSize];
            expDotProductValues = new double[numTopics][alphabet[0].size()];  //<topic,word>
            sumExpValues = new double[numTopics]; // Partition function values per topic 

            String word = elements[0];
            //TODO: I should only take into account words that have wordvectors...
            int wordId = alphabet[0].lookupIndex(word, false);
            if (alphabet[0].lookupIndex(word) != -1) {
                for (int j = 0; j < vectorSize; j++) {
                    typeVectors[wordId][j] = new Double(elements[j + 1]);
                }
            }
            for (String line; (line = br.readLine()) != null;) {
                elements = line.trim().split("\\s+");
                word = elements[0];
                wordId = alphabet[0].lookupIndex(word, false);
                if (alphabet[0].lookupIndex(word) != -1) {

                    for (int j = 0; j < vectorSize; j++) {
                        typeVectors[wordId][j] = new Double(elements[j + 1]);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < alphabet[0].size(); i++) {
            for (int j = 0; j < numTopics; j++) {
                //Arrays.fill(this.typeTopicSimilarity[i][j], (short) 10000);
                if (MatrixOps.absNorm(typeVectors[i]) == 0.0) {
                    System.out.println("The word \"" + alphabet[0].lookupObject(i)
                            + "\" doesn't have a corresponding vector!!!");
                    throw new Exception();
                }
            }
        }
    }

    public void readWordVectorsDB(String SQLLitedb, int vectorSize) //word vectors for text modality
    {

        if (useTypeVectors) {
            this.vectorSize = vectorSize;

            expDotProductValues = new double[numTopics][alphabet[0].size()];  //<topic,word>
            sumExpValues = new double[numTopics]; // Partition function values per topic 

            if (trainTypeVectors) {

            } else {
                this.topicVectors = new double[numTopics][vectorSize];// Vector representations for topics <topic, vector>
                this.typeVectors = new double[alphabet[0].size()][vectorSize];

                Connection connection = null;
                try {
                    connection = DriverManager.getConnection(SQLLitedb);
                    String sql = "";

                    sql = "select word, ColumnId, weight, modality from WordVector order by modality, word, columnId";

                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql);

                    while (rs.next()) {

                        int m = rs.getInt("modality");
                        int w = m == 0 ? alphabet[m].lookupIndex(rs.getString("word"), false) : Integer.parseInt(rs.getString("word"));
                        int c = rs.getInt("ColumnId");
                        double weight = rs.getDouble("weight");
                        if (w != -1) {
                            if (m == -1) {
                                this.topicVectors[w][c] = weight;

                            } else if (m == 0) {
                                this.typeVectors[w][c] = weight;
                            }

                        }

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

                for (int i = 0; i < alphabet[0].size(); i++) {
                    for (int j = 0; j < numTopics; j++) {
                        //Arrays.fill(this.typeTopicSimilarity[i][j],  0);
                        if (MatrixOps.absNorm(typeVectors[i]) == 0.0) {
                            System.out.println("The word \"" + alphabet[0].lookupObject(i)
                                    + "\" doesn't have a corresponding vector!!!");
                            //       throw new Exception();
                        }
                    }

                }

            }
        }
    }

    private void CalcSoftmaxTopicWordProbabilities() {

        double[][] dotProductValues = new double[numTopics][alphabet[0].size()]; //<topic,token>

        for (int topic = 0; topic < numTopics; topic++) {
            double max = -1000000000.0;

            for (int w = 0; w < alphabet[0].size(); w++) {
                //if (MatrixOps.absNorm(typeVectors[w]) != 0.0) // meaning that word vector exists
                //{
                dotProductValues[topic][w] = dotProduct(typeVectors[w], topicVectors[topic]);
                if (dotProductValues[topic][w] > max) {
                    max = dotProductValues[topic][w];
                }
                //}

            }

            for (int w = 0; w < alphabet[0].size(); w++) {
                //if (MatrixOps.absNorm(typeVectors[w]) != 0.0) // meaning that word vector exists
                //{
                expDotProductValues[topic][w] = Math
                        .exp(dotProductValues[topic][w] - max);
                sumExpValues[topic] += expDotProductValues[topic][w];
                //}

            }

        }

    }

    private double[] CalcTopicVectorBasedOnWords(int maxNumWords, int topic) {
        //ArrayList<TreeSet<IDSorter>> topicSortedWords = new ArrayList<TreeSet<IDSorter>>();
        double[] TopicVectorsW = new double[vectorSize];
        ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords(0);

        //int[][] topicTypeCounts = new int[numModalities][numTopics];
        double[] topicsDiscrWeight = calcDiscrWeightWithinTopics(topicSortedWords, true, 0);

        // for (int topic = 0; topic < numTopics; topic++) {
        int topicTypeCount = topicSortedWords.get(topic).size();
        int activeNumWords = Math.min(maxNumWords, 7 * (int) Math.round(topicsDiscrWeight[topic] * topicTypeCount));

        //logger.info("Active NumWords: " + topic + " : " + activeNumWords);
        TreeSet<IDSorter> sortedWords = topicSortedWords.get(topic);
        Iterator<IDSorter> iterator = sortedWords.iterator();

        int wordCnt = 0;
        while (iterator.hasNext() && wordCnt < activeNumWords) {
            IDSorter info = iterator.next();
            MatrixOps.plusEquals(TopicVectorsW, typeVectors[info.getID()], info.getWeight() / tokensPerTopic[0][topic]);
            wordCnt++;
        }

        // }
        return TopicVectorsW;
    }

    public void addInstances(InstanceList[] training, String batchId, int vectorSize, String initModelFile) {

        FastQMVWVParallelTopicModel previousModel = null;
        if (initModelFile != "") {
            previousModel = initFromPreviousRun(initModelFile);
        }

        TObjectIntHashMap<String> entityPosition = new TObjectIntHashMap<String>();
        typeTotals = new int[numModalities][];

        appendMetadata("Statistics for batch:" + batchId);

        Randoms random = null;
        if (randomSeed == -1) {
            random = new Randoms();
        } else {
            random = new Randoms(randomSeed);
        }

        for (Byte m = 0; m < numModalities; m++) {
            alphabet[m] = training[m].getDataAlphabet();
            numTypes[m] = alphabet[m].size();
            typeTotals[m] = new int[numTypes[m]];

            String modInfo = "Modality<" + m + ">[" + (training[m].size() > 0 ? training[m].get(0).getSource().toString() : "-") + "] Size:" + training[m].size() + " Alphabet count: " + numTypes[m];
            logger.info(modInfo);
            appendMetadata(modInfo);

            betaSum[m] = beta[m] * numTypes[m];
            int doc = 0;

            for (Instance instance : training[m]) {
                doc++;
                long iterationStart = System.currentTimeMillis();

                FeatureSequence tokens = (FeatureSequence) instance.getData();
                //docLengthCounts[m][tokens.getLength()]++;
                LabelSequence topicSequence
                        = new LabelSequence(topicAlphabet, new int[tokens.size()]);
//                int size = tokens.size();
//                int[] topics = new int[size]; //topicSequence.getFeatures();
//                for (int position = 0; position < topics.length; position++) {
//                    int type = tokens.getIndexAtPosition(position);
//                    //int topic = ThreadLocalRandom.current().nextInt(numTopics); //random.nextInt(numTopics);
//                    int topic = random.nextInt(numTopics);
//                    topics[position] = topic;
//                    typeTotals[m][type]++;
//
//                }

                //TopicAssignment t = new TopicAssignment(instance, topicSequence);
                TopicAssignment t = new TopicAssignment(instance, topicSequence);

                //data.add(t);
                MixTopicModelTopicAssignment mt;
                String entityId = (String) instance.getName();

                //int index = i == 0 ? -1 : data.indexOf(mt);
                int index = -1;
                //if (i != 0 && (entityPosition.containsKey(entityId))) {

                if (m != 0 && entityPosition.containsKey(entityId)) {

                    index = entityPosition.get(entityId);
                    mt = data.get(index);
                    mt.Assignments[m] = t;

                } else {
                    mt = new MixTopicModelTopicAssignment(entityId, new TopicAssignment[numModalities]);
                    mt.Assignments[m] = t;
                    data.add(mt);
                    index = data.size() - 1;
                    entityPosition.put(entityId, index);
                }

                long elapsedMillis = System.currentTimeMillis() - iterationStart;
                if (doc % 100000 == 0) {
                    logger.info(elapsedMillis + "ms " + "  docNum:" + doc);

                }
            }
        }

        List<Integer> activeTopics = new LinkedList<Integer>();
        for (MixTopicModelTopicAssignment entity : data) {

            for (byte m = 0; m < numModalities; m++) {

                if (m == 0) {
                    activeTopics.clear();
                }

                TopicAssignment document = entity.Assignments[m];
                if (document != null) {

                    FeatureSequence tokens = (FeatureSequence) document.instance.getData();

                    FeatureSequence topicSequence = (FeatureSequence) document.topicSequence;

                    int[] topics = topicSequence.getFeatures();
                    for (int position = 0; position < tokens.size(); position++) {

                        int type = tokens.getIndexAtPosition(position);
                        //int topic = ThreadLocalRandom.current().nextInt(numTopics); //random.nextInt(numTopics);
                        int topic = -1;

                        if (previousModel != null) {
                            int previoustype = (int) previousModel.alphabet[m].lookupIndex(alphabet[m].lookupObject(type), false);
                            if (previoustype != -1) {
                                FTree currentTree = previousModel.trees[m][previoustype];

                                double nextUniform2 = ThreadLocalRandom.current().nextDouble();
                                topic = currentTree.sample(nextUniform2);
                            }
                        }

                        if (topic == -1) {
                            if (m == 0) {
                                topic = random.nextInt(numTopics);
                                activeTopics.add(topic);
                            } else if (activeTopics.size() > 0) {
                                int ind = random.nextInt(activeTopics.size()); //use only topics that are active in modality 0 for each document to avoid local minima and misplaced topic ids
                                topic = activeTopics.get(ind);
                            } else {
                                topic = random.nextInt(numTopics);
                            }
                        }

                        topics[position] = topic;
                        typeTotals[m][type]++;
                    }
                }
            }
        }

        if (useTypeVectors && !trainTypeVectors) {
            readWordVectorsDB(SQLLiteDB, vectorSize);
        }
        initializeHistograms();
        initSpace();
        buildInitialTypeTopicCounts();

        buildFTrees(true);

    }

//    public void initializeFromState(File stateFile) throws IOException {
//        String line;
//        String[] fields;
//
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(stateFile))));
//        line = reader.readLine();
//
//        // Skip some lines starting with "#" that describe the format and specify hyperparameters
//        while (line.startsWith("#")) {
//            line = reader.readLine();
//        }
//
//        fields = line.split(" ");
//
//        for (TopicAssignment document : data) {
//            FeatureSequence tokens = (FeatureSequence) document.instance.getData();
//            FeatureSequence topicSequence = (FeatureSequence) document.topicSequence;
//
//            int[] topics = topicSequence.getFeatures();
//            for (int position = 0; position < tokens.size(); position++) {
//                int type = tokens.getIndexAtPosition(position);
//
//                if (type == Integer.parseInt(fields[3])) {
//                    topics[position] = Integer.parseInt(fields[5]);
//                } else {
//                    System.err.println("instance list and state do not match: " + line);
//                    throw new IllegalStateException();
//                }
//
//                line = reader.readLine();
//                if (line != null) {
//                    fields = line.split(" ");
//                }
//            }
//        }
//
//        initializeHistograms();
//        buildInitialTypeTopicCounts();
//
//    }
    public void initSpace() {
        trees = new FTree[numModalities][];
        typeTopicCounts = new int[numModalities][][];
        tokensPerTopic = new int[numModalities][numTopics];
        maxTypeCount = new int[numModalities];
        typeDiscrWeight = new double[numModalities][]; //<modality, type>

        for (Byte m = 0; m < numModalities; m++) {
            typeTopicCounts[m] = new int[numTypes[m]][numTopics];
            typeDiscrWeight[m] = new double[numTypes[m]];
            trees[m] = new FTree[numTypes[m]];

            //find maxTypeCount needed in countHistogram Optimize Beta
            maxTypeCount[m] = 0;
            for (int type = 0; type < numTypes[m]; type++) {
                if (typeTotals[m][type] > maxTypeCount[m]) {
                    maxTypeCount[m] = typeTotals[m][type];
                }
            }

        }

    }

    public void buildInitialTypeTopicCounts() {

        for (Byte i = 0; i < numModalities; i++) {
            // Clear the topic totals
            Arrays.fill(tokensPerTopic[i], 0);

            // Clear the type/topic counts, only 
            for (int type = 0; type < numTypes[i]; type++) {

                Arrays.fill(typeTopicCounts[i][type], 0);

            }

            Arrays.fill(docLengthCounts[i], 0);
            for (int t = 0; t < numTopics; t++) {
                Arrays.fill(topicDocCounts[i][t], 0);
            }
        }

        Arrays.fill(totalDocsPerModality, 0);
        for (MixTopicModelTopicAssignment entity : data) {
            for (Byte m = 0; m < numModalities; m++) {
                TopicAssignment document = entity.Assignments[m];
                if (document != null) {
                    totalDocsPerModality[m]++;
                    FeatureSequence tokens = (FeatureSequence) document.instance.getData();
                    docLengthCounts[m][tokens.getLength()]++;
                    FeatureSequence topicSequence = (FeatureSequence) document.topicSequence;
                    int[] localTopicCounts = new int[numTopics];
                    int[] topics = topicSequence.getFeatures();
                    for (int position = 0; position < tokens.size(); position++) {

                        int topic = topics[position];

                        if (topic == UNASSIGNED_TOPIC) {
                            continue;
                        }

                        localTopicCounts[topics[position]]++;

                        tokensPerTopic[m][topic]++;

                        int type = tokens.getIndexAtPosition(position);
                        typeTopicCounts[m][type][topic]++;

                    }

                    for (int topic = 0; topic < numTopics; topic++) {
                        topicDocCounts[m][topic][localTopicCounts[topic]]++;
                    }
                }
            }
        }

        //init trees
//        double[] temp = new double[numTopics];
//        for (Byte m = 0; m < numModalities; m++) {
//            for (int w = 0; w < numTypes[m]; ++w) {
//
//                int[] currentTypeTopicCounts = typeTopicCounts[m][w];
//                for (int currentTopic = 0; currentTopic < numTopics; currentTopic++) {
//                    temp[currentTopic] = gamma[m] * alpha[m][currentTopic] * (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);
//
//                }
//
//                //trees[w].init(numTopics);
//                trees[m][w] = new FTree(temp);
//                //reset temp
//                Arrays.fill(temp, 0);
//
//            }
//
//            docSmoothingOnlyMass[m] = 0;
//        }
    }

    public void mergeSimilarTopics(int maxNumWords, TByteArrayList modalities, double mergeSimilarity, int deleteNumTopics) {

        // consider similarity on top numWords
        HashMap<String, SparseVector> labelVectors = new HashMap<String, SparseVector>();
        String labelId = "";
        NormalizedDotProductMetric cosineSimilarity = new NormalizedDotProductMetric();
        //int[] topicMapping = new int[numTopics];
        ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords = new ArrayList<ArrayList<TreeSet<IDSorter>>>(numModalities);

        for (Byte m = 0; m < numModalities; m++) {
            topicSortedWords.add(getSortedWords(m));
        }

        //int[][] topicTypeCounts = new int[numModalities][numTopics];
        double[][] topicsDiscrWeight = calcDiscrWeightWithinTopics(topicSortedWords, true);

        for (int topic = 0; topic < numTopics; topic++) {
            int previousVocabularySize = 0;
            labelId = Integer.toString(topic);
            int[] wordTypes = new int[maxNumWords * modalities.size()];
            double[] weights = new double[maxNumWords * modalities.size()];

            for (Byte m = 0; m < numModalities && modalities.contains(m); m++) {

                int topicTypeCount = topicSortedWords.get(m).get(topic).size();
                int activeNumWords = Math.min(maxNumWords, 7 * (int) Math.round(topicsDiscrWeight[m][topic] * topicTypeCount));

                //logger.info("Active NumWords: " + topic + " : " + activeNumWords);
                TreeSet<IDSorter> sortedWords = topicSortedWords.get(m).get(topic);
                Iterator<IDSorter> iterator = sortedWords.iterator();

                int wordCnt = 0;
                while (iterator.hasNext() && wordCnt < activeNumWords) {

                    IDSorter info = iterator.next();
                    wordTypes[wordCnt] = previousVocabularySize + info.getID();//((String) entity).hashCode();
                    weights[wordCnt] = info.getWeight() / tokensPerTopic[m][topic];
                    wordCnt++;
                }

                previousVocabularySize += maxTypeCount[m];
            }

            labelVectors.put(labelId, new SparseVector(wordTypes, weights, maxNumWords, maxNumWords, true, false, true));
        }

        double similarity = 0;
        //double maxSimilarity = 0;
        String labelTextId;

        //TObjectDoubleHashMap<String> topicSimilarities = new TObjectDoubleHashMap<String>();
//        double[][] topicSimilarities = new double[numTopics][numTopics];
//
//        for (int i = 0; i < numTopics; i++) {
//            Arrays.fill(topicSimilarities[i], 0);
//        }
        //}
        TIntIntHashMap mergedTopics = new TIntIntHashMap();

        for (int t = 0; t < numTopics; t++) {

            for (int t_text = t + 1; t_text < numTopics; t_text++) {
                labelId = Integer.toString(t);
                labelTextId = Integer.toString(t_text);
                SparseVector source = labelVectors.get(labelId);
                SparseVector target = labelVectors.get(labelTextId);
                similarity = 0;
                if (source != null & target != null) {
                    similarity = 1 - Math.abs(cosineSimilarity.distance(source, target)); // the function returns distance not similarity
                }
                if (similarity > mergeSimilarity) {
                    mergedTopics.put(t, t_text);
                    logger.info("Merge topics: " + t + " and " + t_text);
                    for (Byte m = 0; m < numModalities; m++) {
                        alpha[m][t] = 0;
                    }

                }

                //topicSimilarities[t][t_text] = similarity;
            }

        }

        double avgAlpha = 0;
        int cnt = 0;

        for (int kk = 0; kk <= numTopics; kk++) {
            //int k = kactive.get(kk);
            avgAlpha += alpha[0][kk];
            cnt += alpha[0][kk] == 0 ? 0 : 1;

        }

        avgAlpha = avgAlpha / cnt; /// (numTopics - inActiveTopicIndex.size());

        IDSorter[] sortedTopics = new IDSorter[numTopics];
        for (int topic = 0; topic < numTopics; topic++) {
            // Initialize the sorters with big values

            sortedTopics[topic] = new IDSorter(topic, Double.MAX_VALUE);
        }

        List<Integer> deletedTopics = new LinkedList<Integer>();
        double totalCohDiscrDiffWeight = 0;
        int totalCohDiscrDiffWeightcnt = 0;
        for (int kk = 0; kk < numTopics; kk++) {
            if (alpha[0][kk] != 0) {
                //double diffLogWeight = Math.abs(Math.log10(alpha[0][kk]) - Math.log10(avgAlpha));
                //sortedTopics[kk] = new IDSorter(kk, diffLogWeight);
                //sortedTopics[kk] = new IDSorter(kk, topicsDiscrWeight[0][kk] / diffLogWeight);
                if (topicsDiscrWeight[0][kk] != 0) {
                    sortedTopics[kk] = new IDSorter(kk, topicsDiscrWeight[0][kk]);
                    totalCohDiscrDiffWeight += topicsDiscrWeight[0][kk]; // / diffLogWeight;
                    totalCohDiscrDiffWeightcnt++;
                }
                //sortedTopics[kk].set(kk, topicsSkewWeight[0][kk] / Math.abs(Math.log10(alpha[0][kk]) - Math.log10(avgAlpha)));
            }

        }

        double avgDiscrWeight = totalCohDiscrDiffWeight / totalCohDiscrDiffWeightcnt;

        Arrays.sort(sortedTopics);
        for (int j = sortedTopics.length - 1; j >= sortedTopics.length - deleteNumTopics; j--) {
            if (Math.log10(avgDiscrWeight) - Math.log10(sortedTopics[j].getWeight()) > 2) {
                //  if ((Math.log10(alpha[0][sortedTopics[j].getID()]) - Math.log10(avgAlpha)) > 1) {
                deletedTopics.add(sortedTopics[j].getID());
                logger.info("Delete topic: " + sortedTopics[j].getID());
                //  }
            }

        }

        logger.info("Found topics to merge: " + mergedTopics.size());

        if (mergedTopics.size() > 0 || (!deletedTopics.isEmpty())) {
            for (MixTopicModelTopicAssignment entity : data) {
                for (Byte m = 0; m < numModalities; m++) {
                    TopicAssignment document = entity.Assignments[m];

                    if (document != null) {

                        //FeatureSequence tokens = (FeatureSequence) document.instance.getData();
                        FeatureSequence topicSequence = (FeatureSequence) document.topicSequence;
                        int[] topics = topicSequence.getFeatures();

                        for (int position = 0; position < topics.length; position++) {
                            int oldTopic = topics[position];

                            if (deletedTopics.contains(oldTopic)) {

                                topics[position] = FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC;

                            }
                            if (mergedTopics.containsKey(oldTopic)) {
                                topics[position] = mergedTopics.get(oldTopic);
                            }

                        }
                    }
                }
            }
            buildInitialTypeTopicCounts();
        }

    }

    /**
     * Gather statistics on the size of documents and create histograms for use
     * in Dirichlet hyperparameter optimization.
     */
    private void initializeHistograms() {

        int maxTotalAllModalities = 0;
        //int[] maxTokens = new int[numModalities];
        histogramSize = new int[numModalities];
        //double[] avgSize = new double[numModalities];

        Arrays.fill(totalTokens, 0);
        // Arrays.fill(maxTokens, 0);
        Arrays.fill(histogramSize, 0);

        for (MixTopicModelTopicAssignment entity : data) {

            for (Byte i = 0; i < numModalities; i++) {
                int seqLen;
                TopicAssignment document = entity.Assignments[i];
                if (document != null) {
                    FeatureSequence fs = (FeatureSequence) document.instance.getData();
                    seqLen = fs.getLength();

                    if (seqLen > histogramSize[i]) {
                        histogramSize[i] = seqLen;
                    }
                    totalTokens[i] += seqLen;
                }
            }
        }

        for (Byte i = 0; i < numModalities; i++) {
            String infoStr = "Modality<" + i + "> Max tokens per entity: " + histogramSize[i] + ", Total tokens: " + totalTokens[i];
            logger.info(infoStr);
            appendMetadata(infoStr);

            maxTotalAllModalities += histogramSize[i];
        }
        logger.info("max tokens all modalities: " + maxTotalAllModalities);

        docLengthCounts = new int[numModalities][];
        topicDocCounts = new int[numModalities][][];
        for (Byte m = 0; m < numModalities; m++) {
            docLengthCounts[m] = new int[histogramSize[m] + 1];
            topicDocCounts[m] = new int[numTopics][histogramSize[m] + 1];

            for (int t = 0; t < numTopics; t++) {
                Arrays.fill(topicDocCounts[m][t], 0);
            }

        }

//        for (MixTopicModelTopicAssignment entity : data) {
//            for (Byte i = 0; i < numModalities; i++) {
//                TopicAssignment document = entity.Assignments[i];
//                if (document != null) {
//                    FeatureSequence tokens = (FeatureSequence) document.instance.getData();
//                    docLengthCounts[i][tokens.getLength()]++;
//                }
//            }
//        }
    }

//    public void optimizeAlpha(FastQWorkerRunnable[] runnables) {
//
//        // First clear the sufficient statistic histograms
//        //Arrays.fill(docLengthCounts, 0);
////        for (int topic = 0; topic < topicDocCounts.length; topic++) {
////            Arrays.fill(topicDocCounts[topic], 0);
////        }
////
////        for (int thread = 0; thread < numThreads; thread++) {
////            //int[] sourceLengthCounts = runnables[thread].getDocLengthCounts();
////            int[][] sourceTopicCounts = runnables[thread].getTopicDocCounts();
////
//////            for (int count = 0; count < sourceLengthCounts.length; count++) {
//////                if (sourceLengthCounts[count] > 0) {
//////                    docLengthCounts[count] += sourceLengthCounts[count];
//////                    sourceLengthCounts[count] = 0;
//////                }
//////            }
////            for (int topic = 0; topic < numTopics; topic++) {
////
////                if (!usingSymmetricAlpha) {
////                    for (int count = 0; count < sourceTopicCounts[topic].length; count++) {
////                        if (sourceTopicCounts[topic][count] > 0) {
////                            topicDocCounts[topic][count] += sourceTopicCounts[topic][count];
////                            sourceTopicCounts[topic][count] = 0;
////                        }
////                    }
////                } else {
////                    // For the symmetric version, we only need one 
////                    //  count array, which I'm putting in the same 
////                    //  data structure, but for topic 0. All other
////                    //  topic histograms will be empty.
////                    // I'm duplicating this for loop, which 
////                    //  isn't the best thing, but it means only checking
////                    //  whether we are symmetric or not numTopics times, 
////                    //  instead of numTopics * longest document length.
////                    for (int count = 0; count < sourceTopicCounts[topic].length; count++) {
////                        if (sourceTopicCounts[topic][count] > 0) {
////                            topicDocCounts[0][count] += sourceTopicCounts[topic][count];
////                            //			 ^ the only change
////                            sourceTopicCounts[topic][count] = 0;
////                        }
////                    }
////                }
////            }
////        }
//        if (usingSymmetricAlpha) {
//            alphaSum[0] = Dirichlet.learnSymmetricConcentration(topicDocCounts[0],
//                    docLengthCounts,
//                    numTopics,
//                    alphaSum[0]);
//            for (int topic = 0; topic < numTopics; topic++) {
//                alpha[topic] = alphaSum[0] / numTopics;
//            }
//        } else {
//            try {
//                alphaSum[0] = Dirichlet.learnParameters(alpha, topicDocCounts, docLengthCounts, 1.001, 1.0, 1);
//            } catch (RuntimeException e) {
//                // Dirichlet optimization has become unstable. This is known to happen for very small corpora (~5 docs).
//                logger.warning("Dirichlet optimization has become unstable. Resetting to alpha_t = 1.0.");
//                alphaSum[0] = numTopics;
//                for (int topic = 0; topic < numTopics; topic++) {
//                    alpha[topic] = 1.0;
//                }
//            }
//        }
//
//        String alphaStr = "";
//        for (int topic = 0; topic < numTopics; topic++) {
//            alphaStr += formatter.format(alpha[topic]) + " ";
//        }
//
//        logger.info("[Alpha: [" + alphaStr + "] ");
//    }
//    public void optimizeBeta(FastQWorkerRunnable[] runnables) {
//        // The histogram starts at count 0, so if all of the
//        //  tokens of the most frequent type were assigned to one topic,
//        //  we would need to store a maxTypeCount + 1 count.
//        int[] countHistogram = new int[maxTypeCount + 1];
//
//        //  Now count the number of type/topic pairs that have
//        //  each number of tokens.
//        for (int type = 0; type < numTypes; type++) {
//
//            int[] counts = typeTopicCounts[type];
//
//            for (int topic = 0; topic < numTopics; topic++) {
//                int count = counts[topic];
//                if (count > 0) {
//                    countHistogram[count]++;
//                }
//            }
//        }
//
//        // Figure out how large we need to make the "observation lengths"
//        //  histogram.
//        int maxTopicSize = 0;
//        for (int topic = 0; topic < numTopics; topic++) {
//            if (tokensPerTopic[topic] > maxTopicSize) {
//                maxTopicSize = tokensPerTopic[topic];
//            }
//        }
//
//        // Now allocate it and populate it.
//        int[] topicSizeHistogram = new int[maxTopicSize + 1];
//        for (int topic = 0; topic < numTopics; topic++) {
//            topicSizeHistogram[tokensPerTopic[topic]]++;
//        }
//
//        betaSum[0] = Dirichlet.learnSymmetricConcentration(countHistogram,
//                topicSizeHistogram,
//                numTypes,
//                betaSum[0]);
//        beta[0] = betaSum[0] / numTypes;
//
//        //TODO: copy/update trees in threads
//        logger.info("[beta[0]: " + formatter.format(beta[0]) + "] ");
//        // Now publish the new value
////        for (int thread = 0; thread < numThreads; thread++) {
////            runnables[thread].resetBeta(beta[0], betaSum);
////        }
//
//    }
    public void estimate() throws IOException {

        long startTime = System.currentTimeMillis();
        final CyclicBarrier barrier = new CyclicBarrier(numThreads + 2);//one for the current thread and one for the updater

        FastQMVWVWorkerRunnable[] runnables = new FastQMVWVWorkerRunnable[numThreads];

        int docsPerThread = data.size() / numThreads;
        int offset = 0;

        //pDistr_Var = new double[numModalities][numModalities][data.size()];
        for (byte i = 0; i < numModalities; i++) {
            Arrays.fill(this.p_a[i], 0.2d);
            Arrays.fill(this.p_b[i], 1d);
        }

        for (int thread = 0; thread < numThreads; thread++) {

            // some docs may be missing at the end due to integer division
            if (thread == numThreads - 1) {
                docsPerThread = data.size() - offset;
            }

            Randoms random = null;
            if (randomSeed == -1) {
                random = new Randoms();
            } else {
                random = new Randoms(randomSeed);
            }

            runnables[thread] = new FastQMVWVWorkerRunnable(
                    numTopics,
                    numModalities,
                    alpha, alphaSum, beta, betaSum, gamma,
                    docSmoothingOnlyMass,
                    docSmoothingOnlyCumValues,
                    random, data,
                    typeTopicCounts, tokensPerTopic,
                    offset,
                    docsPerThread, trees,
                    thread,
                    p_a,
                    p_b,
                    //queues.get(thread), 
                    barrier,
                    inActiveTopicIndex,
                    expDotProductValues,
                    sumExpValues
            );

            //runnables[thread].initializeAlphaStatistics(docLengthCounts.length);
            offset += docsPerThread;

            //runnables[thread].makeOnlyThread();
        }

        Randoms randomUpd = null;
        if (randomSeed == -1) {
            randomUpd = new Randoms();
        } else {
            randomUpd = new Randoms(randomSeed);
        }

        FastQMVWVUpdaterRunnable updater = new FastQMVWVUpdaterRunnable(
                typeTopicCounts,
                tokensPerTopic,
                trees,
                //queues,
                alpha, alphaSum,
                beta, betaSum, gamma,
                docSmoothingOnlyMass,
                docSmoothingOnlyCumValues,
                barrier,
                numTopics,
                numModalities,
                docLengthCounts,
                topicDocCounts,
                numTypes,
                maxTypeCount,
                randomUpd,
                inActiveTopicIndex,
                expDotProductValues,
                sumExpValues
        //        , betaSmoothingTree
        );

//         List<Queue<FastQDelta>> queues = new ArrayList<Queue<FastQDelta>>(numThreads);
//            for (int thread = 0; thread < numThreads; thread++) {
//               // queues.add(new ConcurrentLinkedQueue<FastQDelta>());
//                queues.add(new LinkedBlockingQueue<FastQDelta>());
//            }
//            
        ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);

        for (int iteration = 1; iteration <= numIterations; iteration++) {

            List<Queue<FastQDelta>> queues = new ArrayList<Queue<FastQDelta>>(numThreads);
            for (int thread = 0; thread < numThreads; thread++) {
                //queues.add(new ConcurrentLinkedQueue<FastQDelta>());

                queues.add(new LinkedBlockingQueue<FastQDelta>());
            }

            long iterationStart = System.currentTimeMillis();

            if (showTopicsInterval != 0 && iteration != 0 && iteration % showTopicsInterval == 0) {
                logger.info("\n" + displayTopWords(wordsPerTopic, 5, false));
            }

            if (saveStateInterval != 0 && iteration % saveStateInterval == 0) {
                this.printState(new File(stateFilename + '.' + iteration));
            }

            if (saveModelInterval != 0 && iteration % saveModelInterval == 0) {
                this.write(new File(modelFilename + '.' + iteration));
            }

            updater.setOptimizeParams(false);
            if (iteration < burninPeriod && numModalities > 1) {
                for (byte i = 0; i < numModalities; i++) {
                    Arrays.fill(this.p_a[i], Math.min((double) iteration / 100 + 0.3d, 1.1d));

                    //Arrays.fill(this.p_b[i], 1d);
                }
                logger.info("common p_a: " + formatter.format(this.p_a[0][1]));
            } else if (iteration > burninPeriod && optimizeInterval != 0
                    && iteration % optimizeInterval == 0) {
                //updater.setOptimizeParams(true);
                optimizeP(iteration + optimizeInterval > numIterations);
                //merge similar topics
                TByteArrayList modalities = new TByteArrayList();
                modalities.add((byte) 0);

                //if (iteration >= burninPeriod + optimizeInterval) {
//                    mergeSimilarTopics(40, modalities, 0.8, 0); //TODO: implement additional topic similarity based on Word Vectors
                //              }
                optimizeDP();
                optimizeGamma();
                optimizeBeta();
                if (useTypeVectors) {
                    if (trainTypeVectors) {

                        int windowSizeOption = 5;
                        int numSamples = 5;
                        TopicWordEmbeddings matrix = new TopicWordEmbeddings(alphabet[0], vectorSize, 10, windowSizeOption, numTopics);
                        matrix.queryWord = "mining";
                        matrix.countWords(data, 0.0001); //Sampling factor : "Down-sample words that account for more than ~2.5x this proportion or the corpus."
                        matrix.train(data, numThreads, numSamples, 5);//numOfIterations

                        topicVectors = matrix.getTopicVectors();
                        typeVectors = matrix.getWordVectors(); //TODO: Extend to all modalities
                    }
                    //CalcTopicTypeVectorSimilarities(40);
                    CalcSoftmaxTopicWordProbabilities();
                    useVectorsLambda = vectorsLambda; // Incorporate vector based distributions 
                    updater.setUseVectorsLambda(useVectorsLambda);
                    for (int thread = 0; thread < numThreads; thread++) {
                        runnables[thread].setUseVectorsLambda(useVectorsLambda);
                    }
                }
                buildFTrees(false);
            }

            updater.setQueues(queues);
            executor.submit(updater);
            // if (numThreads > 1) {
            // Submit runnables to thread pool
            for (int thread = 0; thread < numThreads; thread++) {

//                if (iteration > burninPeriod && optimizeInterval != 0
//                        && iteration % saveSampleInterval == 0) {
//                    runnables[thread].collectAlphaStatistics();
//                }
                runnables[thread].setQueue(queues.get(thread));

                logger.info("submitting thread " + thread);
                executor.submit(runnables[thread]);
                //runnables[thread].run();
            }

            try {
                barrier.await();
            } catch (InterruptedException e) {
                System.out.println("Main Thread interrupted!");
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                System.out.println("Main Thread interrupted!");
                e.printStackTrace();
            }

//            // I'm getting some problems that look like 
//            //  a thread hasn't started yet when it is first
//            //  polled, so it appears to be finished. 
//            // This only occurs in very short corpora.
//            try {
//                Thread.sleep(20);
//            } catch (InterruptedException e) {
//
//            }
//
//            //TODO: use Barrier here 
//            boolean finished = false;
//            while (!finished) {
//
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//
//                }
//
//                finished = true;
//
//                // Are all the threads done?
//                for (int thread = 0; thread < numThreads; thread++) {
//                    //logger.info("thread " + thread + " done? " + runnables[thread].isFinished);
//                    finished = finished && runnables[thread].isFinished;
//                }
//                
//                finished = finished && updater.isFinished;
//
//            }
            long elapsedMillis = System.currentTimeMillis() - iterationStart;
            if (elapsedMillis < 5000) {
                logger.info(elapsedMillis + "ms ");
            } else {
                logger.info((elapsedMillis / 1000) + "s ");
            }

//            String alphaStr = "";
//            for (int topic = 0; topic < numTopics; topic++) {
//                alphaStr += topic + ":" + formatter.format(tokensPerTopic[0][topic]) + " ";
//            }
//
//            logger.info(alphaStr);
//            if (iteration > burninPeriod && optimizeInterval != 0
//                    && iteration % optimizeInterval == 0) {
//
//                //optimizeAlpha(runnables);
//                //optimizeBeta(runnables);
//
//                //recalc trees for multi threaded recalc every time .. for single threaded only when beta[0] (or alpha in not cyvling proposal) is changing
//                //recalcTrees();
//
//                logger.info("[O " + (System.currentTimeMillis() - iterationStart) + "] ");
//            }
            if (iteration % 10 == 0) {
                if (printLogLikelihood) {
                    for (Byte i = 0; i < numModalities; i++) {
                        //Arrays.fill(this.p_a[i], (double) (iteration / 100));
                        //Arrays.fill(this.p_b[i], 1d);

                        double ll = modelLogLikelihood()[i] / totalTokens[i];
                        perplexities[i][iteration / 10] = ll;
                        logger.info("<" + iteration + "> modality<" + i + "> LL/token: " + formatter.format(ll)); //LL for eachmodality

                        int totalCnt = FastQMVWVWorkerRunnable.newMassCnt.get() + FastQMVWVWorkerRunnable.topicDocMassCnt.get() + FastQMVWVWorkerRunnable.wordFTreeMassCnt.get();

                        logger.info("Sampling newMass:" + formatter.format((double) FastQMVWVWorkerRunnable.newMassCnt.get() / totalCnt)
                                + " topicDocMassCnt:" + formatter.format((double) FastQMVWVWorkerRunnable.topicDocMassCnt.get() / totalCnt)
                                + " wordFTreeMassCnt:" + formatter.format((double) FastQMVWVWorkerRunnable.wordFTreeMassCnt.get() / totalCnt)); //LL for eachmodality

                        if (iteration + 10 > numIterations) {
                            appendMetadata("Modality<" + i + "> LL/token: " + formatter.format(ll)); //LL for eachmodality
                            //logger.info("[alphaSum[" + i + "]: " + formatter.format(alphaSum[i]) + "] ");
                        }
                    }
                } else {
                    logger.info("<" + iteration + ">");
                }
            }

            FastQMVWVWorkerRunnable.newMassCnt.set(0);
            FastQMVWVWorkerRunnable.topicDocMassCnt.set(0);
            FastQMVWVWorkerRunnable.wordFTreeMassCnt.set(0);

        }

        executor.shutdownNow();

        long seconds = Math.round((System.currentTimeMillis() - startTime) / 1000.0);
        long minutes = seconds / 60;
        seconds %= 60;
        long hours = minutes / 60;
        minutes %= 60;
        long days = hours / 24;
        hours %= 24;

        StringBuilder timeReport = new StringBuilder();
        timeReport.append("\nTotal time: ");
        if (days != 0) {
            timeReport.append(days);
            timeReport.append(" days ");
        }
        if (hours != 0) {
            timeReport.append(hours);
            timeReport.append(" hours ");
        }
        if (minutes != 0) {
            timeReport.append(minutes);
            timeReport.append(" minutes ");
        }
        timeReport.append(seconds);
        timeReport.append(" seconds");

        logger.info(timeReport.toString());
    }

    public void printTopWords(File file, int numWords, int numLabels, boolean useNewLines) throws IOException {
        PrintStream out = new PrintStream(file);
        printTopWords(out, numWords, numLabels, useNewLines);
        out.close();
    }

    public void saveExperiment(String SQLLiteDB, String experimentId, String experimentDescription) {

        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();
                //statement.executeUpdate("drop table if exists TopicAnalysis");
                //statement.executeUpdate("create table if not exists Experiment (ExperimentId nvarchar(50), Description nvarchar(200), Metadata nvarchar(500), InitialSimilarity Double, PhraseBoost Integer) ");
                //String deleteSQL = String.format("Delete from Experiment where  ExperimentId = '%s'", experimentId);
                //statement.executeUpdate(deleteSQL);
//TODO topic analysis don't exist here
                String boostSelect = String.format("select  \n"
                        + " a.experimentid, PhraseCnts, textcnts, textcnts/phrasecnts as boost\n"
                        + "from \n"
                        + "(select experimentid, itemtype, avg(counts) as PhraseCnts from topicanalysis\n"
                        + "where itemtype=-1\n"
                        + "group by experimentid, itemtype) a inner join\n"
                        + "(select experimentid, itemtype, avg(counts) as textcnts from topicanalysis\n"
                        + "where itemtype=0  and ExperimentId = '%s' \n"
                        + "group by experimentid, itemtype) b on a.experimentId=b.experimentId\n"
                        + "order by a.experimentId;", experimentId);
                float boost = 70;
                ResultSet rs = statement.executeQuery(boostSelect);
                while (rs.next()) {
                    boost = rs.getFloat("boost");
                }

//                String similaritySelect = String.format("select experimentid, avg(avgent) as avgSimilarity, avg(counts) as avgLinks, count(*) as EntitiesCnt\n"
//                        + "from( \n"
//                        + "select experimentid, avg(similarity) as avgent, count(similarity) as counts\n"
//                        + "from entitysimilarity\n"
//                        + "where similarity>0.65 and ExperimentId = '%s' group by experimentid, entityid1)\n"
//                        + "group by experimentid", experimentId);
                PreparedStatement bulkInsert = null;
                String sql = "insert into Experiment (ExperimentId  ,    Description,    Metadata  ,    InitialSimilarity,    PhraseBoost) values(?,?,?, ?, ? );";

                try {
                    connection.setAutoCommit(false);
                    bulkInsert = connection.prepareStatement(sql);

                    bulkInsert.setString(1, experimentId);
                    bulkInsert.setString(2, experimentDescription);
                    bulkInsert.setString(3, expMetadata.toString());
                    bulkInsert.setDouble(4, 0.6);
                    bulkInsert.setInt(5, Math.round(boost));

                    bulkInsert.executeUpdate();

                    connection.commit();

                } catch (SQLException e) {

                    System.err.println(e.getMessage());
                    if (connection != null) {
                        try {

                            System.err.print("Transaction is being rolled back");
                            connection.rollback();
                        } catch (SQLException excep) {
                            System.err.print("Error in insert experiment details");
                        }
                    }
                } finally {

                    if (bulkInsert != null) {
                        bulkInsert.close();
                    }
                    connection.setAutoCommit(true);
                }
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

    public void saveTopics(String SQLLiteDB, String experimentId, String batchId) {

        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();

                //statement.executeUpdate("create table if not exists TopicDetails (TopicId integer, ItemType integer,  Weight double, TotalTokens int, BatchId TEXT,ExperimentId nvarchar(50)) ");
                //String deleteSQL = String.format("Delete from TopicDetails where  ExperimentId = '%s'", experimentId);
                //statement.executeUpdate(deleteSQL);
                String topicDetailInsertsql = "insert into TopicDetails values(?,?,?,?,?,? );";
                PreparedStatement bulkTopicDetailInsert = null;

                try {
                    connection.setAutoCommit(false);
                    bulkTopicDetailInsert = connection.prepareStatement(topicDetailInsertsql);

                    for (int topic = 0; topic < numTopics; topic++) {
                        for (Byte m = 0; m < numModalities; m++) {

                            bulkTopicDetailInsert.setInt(1, topic);
                            bulkTopicDetailInsert.setInt(2, m);
                            bulkTopicDetailInsert.setDouble(3, alpha[m][topic]);
                            bulkTopicDetailInsert.setInt(4, tokensPerTopic[m][topic]);
                            bulkTopicDetailInsert.setString(5, batchId);
                            bulkTopicDetailInsert.setString(6, experimentId);

                            bulkTopicDetailInsert.executeUpdate();
                        }
                    }

                    connection.commit();

                } catch (SQLException e) {
                    System.err.println(e.getMessage());

                    if (connection != null) {
                        try {
                            System.err.print("Transaction is being rolled back");
                            connection.rollback();
                        } catch (SQLException excep) {
                            System.err.print("Error in insert topic details");
                        }
                    }
                } finally {

                    if (bulkTopicDetailInsert != null) {
                        bulkTopicDetailInsert.close();
                    }
                    connection.setAutoCommit(true);
                }

                //statement.executeUpdate("drop table if exists TopicAnalysis");
                //statement.executeUpdate("create table if not exists TopicAnalysis (TopicId integer, ItemType integer, Item nvarchar(100), Counts double,  BatchId TEXT, ExperimentId nvarchar(50)) ");
                //deleteSQL = String.format("Delete from TopicAnalysis where  ExperimentId = '%s'", experimentId);
                //statement.executeUpdate(deleteSQL);
                PreparedStatement bulkInsert = null;
                String sql = "insert into TopicAnalysis values(?,?,?,?,?,?);";

                try {
                    connection.setAutoCommit(false);
                    bulkInsert = connection.prepareStatement(sql);

                    ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords = new ArrayList<ArrayList<TreeSet<IDSorter>>>(numModalities);

                    for (Byte m = 0; m < numModalities; m++) {
                        topicSortedWords.add(getSortedWords(m));
                    }
                    //double[][] topicsDiscrWeight = calcDiscrWeightWithinTopics(topicSortedWords, true);

                    for (int topic = 0; topic < numTopics; topic++) {
                        for (Byte m = 0; m < numModalities; m++) {

                            TreeSet<IDSorter> sortedWords = topicSortedWords.get(m).get(topic);

                            int word = 1;
                            Iterator<IDSorter> iterator = sortedWords.iterator();

                            //int activeNumWords = Math.min(40, 7 * (int) Math.round(topicsDiscrWeight[m][topic] * topicTypeCount));
                            while (iterator.hasNext() && word < 50) {
                                IDSorter info = iterator.next();
                                bulkInsert.setInt(1, topic);
                                bulkInsert.setInt(2, m);
                                bulkInsert.setString(3, alphabet[m].lookupObject(info.getID()).toString());
                                bulkInsert.setDouble(4, info.getWeight());
                                bulkInsert.setString(5, batchId);
                                bulkInsert.setString(6, experimentId);
                                //bulkInsert.setDouble(6, 1);
                                bulkInsert.executeUpdate();

                                word++;
                            }

                        }

                    }

                    // also find and write phrases 
                    TObjectIntHashMap<String>[] phrases = findTopicPhrases();

                    for (int ti = 0; ti < numTopics; ti++) {

                        // Print phrases
                        Object[] keys = phrases[ti].keys();
                        int[] values = phrases[ti].values();
                        double counts[] = new double[keys.length];
                        for (int i = 0; i < counts.length; i++) {
                            counts[i] = values[i];
                        }
                        // double countssum = MatrixOps.sum(counts);
                        Alphabet alph = new Alphabet(keys);
                        RankedFeatureVector rfv = new RankedFeatureVector(alph, counts);
                        int max = rfv.numLocations() < 20 ? rfv.numLocations() : 20;
                        for (int ri = 0; ri < max; ri++) {
                            int fi = rfv.getIndexAtRank(ri);

                            double count = counts[fi];// / countssum;
                            String phraseStr = alph.lookupObject(fi).toString();

                            bulkInsert.setInt(1, ti);
                            bulkInsert.setInt(2, -1);
                            bulkInsert.setString(3, phraseStr);
                            bulkInsert.setDouble(4, count);
                            bulkInsert.setString(5, batchId);
                            bulkInsert.setString(6, experimentId);
                            //bulkInsert.setDouble(6, 1);
                            bulkInsert.executeUpdate();
                        }

                    }
                    connection.commit();
//                if (!sql.equals("")) {
//                    statement.executeUpdate(sql);
//                }

//                    if (!sql.equals("")) {
//                        statement.executeUpdate(sql);
//                    }
                } catch (SQLException e) {

                    System.err.print(e.getMessage());
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

    /**
     * Return an array of sorted sets (one set per topic). Each set contains
     * IDSorter objects with integer keys into the alphabet. To get direct
     * access to the Strings, use getTopWords().
     */
    public ArrayList<TreeSet<IDSorter>> getSortedWords(int modality) {

        ArrayList<TreeSet<IDSorter>> topicSortedWords = new ArrayList<TreeSet<IDSorter>>(numTopics);

        // Initialize the tree sets
        for (int topic = 0; topic < numTopics; topic++) {
            TreeSet<IDSorter> topicTreeSet = new TreeSet<IDSorter>();
            topicSortedWords.add(topicTreeSet);
            // Collect counts
            for (int type = 0; type < numTypes[modality]; type++) {
                int cnt = typeTopicCounts[modality][type][topic];
                if (cnt > 0) {
                    topicTreeSet.add(new IDSorter(type, cnt));
                }
            }
        }

        return topicSortedWords;
    }

    /**
     * Return an array (one element for each topic) of arrays of words, which
     * are the most probable words for that topic in descending order. These are
     * returned as Objects, but will probably be Strings.
     *
     * @param numWords The maximum length of each topic's array of words (may be
     * less).
     */
    public Object[][] getTopWords(int numWords, int modality) {

        ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords(modality);
        Object[][] result = new Object[numTopics][];

        for (int topic = 0; topic < numTopics; topic++) {

            TreeSet<IDSorter> sortedWords = topicSortedWords.get(topic);

            // How many words should we report? Some topics may have fewer than
            //  the default number of words with non-zero weight.
            int limit = numWords;
            if (sortedWords.size() < numWords) {
                limit = sortedWords.size();
            }

            result[topic] = new Object[limit];

            Iterator<IDSorter> iterator = sortedWords.iterator();
            for (int i = 0; i < limit; i++) {
                IDSorter info = iterator.next();
                result[topic][i] = alphabet[modality].lookupObject(info.getID());
            }
        }

        return result;
    }

    public void printTopWords(PrintStream out, int numWords, int numLabels, boolean usingNewLines) {
        out.print(displayTopWords(numWords, numLabels, usingNewLines));
    }

    public String displayTopWords(int numWords, int numLabels, boolean usingNewLines) {

        StringBuilder out = new StringBuilder();
        ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords = new ArrayList<ArrayList<TreeSet<IDSorter>>>(4);

        for (Byte m = 0; m < numModalities; m++) {
            topicSortedWords.add(getSortedWords(m));
        }
        // Print results for each topic

        for (int topic = 0; topic < numTopics; topic++) {
            for (Byte m = 0; m < numModalities; m++) {
                TreeSet<IDSorter> sortedWords = topicSortedWords.get(m).get(topic);

                int word = 1;
                Iterator<IDSorter> iterator = sortedWords.iterator();
                if (usingNewLines) {
                    out.append(topic + "\t" + formatter.format(alpha[m][topic]) + "\n");
                    while (iterator.hasNext() && word < numWords) {
                        IDSorter info = iterator.next();
                        out.append(alphabet[m].lookupObject(info.getID()) + "\t" + formatter.format(info.getWeight()) + "\n");
                        word++;
                    }

                } else {
                    out.append(topic + "\t" + formatter.format(alpha[m][topic]) + "\t");

                    while (iterator.hasNext() && word < numWords) {
                        IDSorter info = iterator.next();
                        out.append(alphabet[m].lookupObject(info.getID()) + "; ");
                        word++;
                    }

                }
            }
            out.append("\n");
        }
        return out.toString();
    }

    public void topicXMLReport(PrintWriter out, int numWords, int numLabels) {

        ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords = new ArrayList<ArrayList<TreeSet<IDSorter>>>(4);

        for (Byte m = 0; m < numModalities; m++) {
            topicSortedWords.add(getSortedWords(m));
        }
        out.println("<?xml version='1.0' ?>");
        out.println("<topicModel>");
        for (int topic = 0; topic < numTopics; topic++) {
            for (Byte m = 0; m < numModalities; m++) {
                out.println("  <topic id='" + topic + "' alpha='" + alpha[m][topic] + "' modality='" + m
                        + "' totalTokens='" + tokensPerTopic[m][topic]
                        + "'>");
                int word = 1;
                Iterator<IDSorter> iterator = topicSortedWords.get(m).get(topic).iterator();
                while (iterator.hasNext() && word <= numWords) {
                    IDSorter info = iterator.next();
                    out.println("	<word rank='" + word + "'>"
                            + alphabet[m].lookupObject(info.getID())
                            + "</word>");
                    word++;
                }

            }
            out.println("  </topic>");
        }
        out.println("</topicModel>");
    }

    public TObjectIntHashMap<String>[] findTopicPhrases() {
        int numTopics = this.getNumTopics();

        TObjectIntHashMap<String>[] phrases = new TObjectIntHashMap[numTopics];
        Alphabet alphabet = this.getAlphabet()[0];

        // Get counts of phrases in topics
        // Search bigrams within corpus to see if they have been assigned to the same topic, adding them to topic phrases
        for (int ti = 0; ti < numTopics; ti++) {
            phrases[ti] = new TObjectIntHashMap<String>();
        }
        for (int di = 0; di < this.getData().size(); di++) {

            TopicAssignment t = this.getData().get(di).Assignments[0];
            if (t != null) {
                Instance instance = t.instance;
                FeatureSequence fvs = (FeatureSequence) instance.getData();
                boolean withBigrams = false;
                if (fvs instanceof FeatureSequenceWithBigrams) {
                    withBigrams = true;
                }
                int prevtopic = -1;
                int prevfeature = -1;
                int topic = -1;
                StringBuffer sb = null;
                int feature = -1;
                int doclen = fvs.size();
                for (int pi = 0; pi < doclen; pi++) {
                    feature = fvs.getIndexAtPosition(pi);
                    topic = t.topicSequence.getIndexAtPosition(pi);
                    if (topic == prevtopic && (!withBigrams || ((FeatureSequenceWithBigrams) fvs).getBiIndexAtPosition(pi) != -1)) {
                        if (sb == null) {
                            sb = new StringBuffer(alphabet.lookupObject(prevfeature).toString() + " " + alphabet.lookupObject(feature));
                        } else {
                            sb.append(" ");
                            sb.append(alphabet.lookupObject(feature));
                        }
                    } else if (sb != null) {
                        String sbs = sb.toString();
                        //logger.info ("phrase:"+sbs);
                        if (phrases[prevtopic].get(sbs) == 0) {
                            phrases[prevtopic].put(sbs, 0);
                        }
                        phrases[prevtopic].increment(sbs);
                        prevtopic = prevfeature = -1;
                        sb = null;
                    } else {
                        prevtopic = topic;
                        prevfeature = feature;
                    }
                }
            }
        }

        return phrases;
    }

    public void topicPhraseXMLReport(PrintWriter out, int numWords) {

        //Phrases only for modality 0 --> text
        int numTopics = this.getNumTopics();
        Alphabet alphabet = this.getAlphabet()[0];

        TObjectIntHashMap<String>[] phrases = findTopicPhrases();
        // phrases[] now filled with counts

        // Now start printing the XML
        out.println("<?xml version='1.0' ?>");
        out.println("<topics>");

        ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords(0);
        double[] probs = new double[alphabet.size()];
        for (int ti = 0; ti < numTopics; ti++) {
            out.print("  <topic id=\"" + ti + "\" alpha=\"" + alpha[0][ti]
                    + "\" totalTokens=\"" + tokensPerTopic[0][ti] + "\" ");

            // For gathering <term> and <phrase> output temporarily 
            // so that we can get topic-title information before printing it to "out".
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            PrintStream pout = new PrintStream(bout);
            // For holding candidate topic titles
            AugmentableFeatureVector titles = new AugmentableFeatureVector(new Alphabet());

            // Print words
            int word = 1;
            Iterator<IDSorter> iterator = topicSortedWords.get(ti).iterator();
            while (iterator.hasNext() && word < numWords) {
                IDSorter info = iterator.next();
                pout.println("	<word weight=\"" + (info.getWeight() / tokensPerTopic[0][ti]) + "\" count=\"" + Math.round(info.getWeight()) + "\">"
                        + alphabet.lookupObject(info.getID())
                        + "</word>");
                word++;
                if (word < 20) // consider top 20 individual words as candidate titles
                {
                    titles.add(alphabet.lookupObject(info.getID()), info.getWeight());
                }
            }

            /*
             for (int type = 0; type < alphabet.size(); type++)
             probs[type] = this.getCountFeatureTopic(type, ti) / (double)this.getCountTokensPerTopic(ti);
             RankedFeatureVector rfv = new RankedFeatureVector (alphabet, probs);
             for (int ri = 0; ri < numWords; ri++) {
             int fi = rfv.getIndexAtRank(ri);
             pout.println ("	  <term weight=\""+probs[fi]+"\" count=\""+this.getCountFeatureTopic(fi,ti)+"\">"+alphabet.lookupObject(fi)+	"</term>");
             if (ri < 20) // consider top 20 individual words as candidate titles
             titles.add(alphabet.lookupObject(fi), this.getCountFeatureTopic(fi,ti));
             }
             */
            // Print phrases
            Object[] keys = phrases[ti].keys();
            int[] values = phrases[ti].values();
            double counts[] = new double[keys.length];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = values[i];
            }
            double countssum = MatrixOps.sum(counts);
            Alphabet alph = new Alphabet(keys);
            RankedFeatureVector rfv = new RankedFeatureVector(alph, counts);
            int max = rfv.numLocations() < numWords ? rfv.numLocations() : numWords;
            for (int ri = 0; ri < max; ri++) {
                int fi = rfv.getIndexAtRank(ri);
                pout.println("	<phrase weight=\"" + counts[fi] / countssum + "\" count=\"" + values[fi] + "\">" + alph.lookupObject(fi) + "</phrase>");
                // Any phrase count less than 20 is simply unreliable
                if (ri < 20 && values[fi] > 20) {
                    titles.add(alph.lookupObject(fi), 100 * values[fi]); // prefer phrases with a factor of 100 
                }
            }

            // Select candidate titles
            StringBuffer titlesStringBuffer = new StringBuffer();
            rfv = new RankedFeatureVector(titles.getAlphabet(), titles);
            int numTitles = 10;
            for (int ri = 0; ri < numTitles && ri < rfv.numLocations(); ri++) {
                // Don't add redundant titles
                if (titlesStringBuffer.indexOf(rfv.getObjectAtRank(ri).toString()) == -1) {
                    titlesStringBuffer.append(rfv.getObjectAtRank(ri));
                    if (ri < numTitles - 1) {
                        titlesStringBuffer.append(", ");
                    }
                } else {
                    numTitles++;
                }
            }
            out.println("titles=\"" + titlesStringBuffer.toString() + "\">");
            out.print(bout.toString());
            out.println("  </topic>");
        }
        out.println("</topics>");
    }

    /**
     * Write the internal representation of type-topic counts (count/topic pairs
     * in descending order by count) to a file.
     */
    public void printTypeTopicCounts(File file) throws IOException {
        PrintWriter out = new PrintWriter(new FileWriter(file));
        for (Byte m = 0; m < numModalities; m++) {
            for (int type = 0; type < numTypes[m]; type++) {

                StringBuilder buffer = new StringBuilder();

                buffer.append(type + " " + alphabet[m].lookupObject(type));

                int[] topicCounts = typeTopicCounts[m][type];

                int index = 0;
                while (index < topicCounts.length) {

                    int count = topicCounts[index];

                    buffer.append(" " + index + ":" + count);

                    index++;
                }

                out.println(buffer);
            }
        }
        out.close();
    }

    public void printTopicWordWeights(File file) throws IOException {
        PrintWriter out = new PrintWriter(new FileWriter(file));
        printTopicWordWeights(out);
        out.close();
    }

    /**
     * Print an unnormalized weight for every word in every topic. Most of these
     * will be equal to the smoothing parameter beta[0].
     */
    public void printTopicWordWeights(PrintWriter out) throws IOException {
        // Probably not the most efficient way to do this...

        for (int topic = 0; topic < numTopics; topic++) {
            for (Byte m = 0; m < numModalities; m++) {
                for (int type = 0; type < numTypes[m]; type++) {

                    //int[] topicCounts = typeTopicCounts[type];
                    double weight = beta[m];
                    weight += typeTopicCounts[m][type][topic];

                    out.println(topic + "\t" + alphabet[m].lookupObject(type) + "\t" + weight);

                }
            }
        }
    }

    /**
     * Get the smoothed distribution over topics for a training instance.
     */
    public double[] getTopicProbabilities(int instanceID, byte modality) {
        LabelSequence topics = data.get(instanceID).Assignments[modality].topicSequence;
        return getTopicProbabilities(topics, modality);
    }

    /**
     * Get the smoothed distribution over topics for a topic sequence, which may
     * be from the training set or from a new instance with topics assigned by
     * an inferencer.
     */
    public double[] getTopicProbabilities(LabelSequence topics, byte modality) {
        double[] topicDistribution = new double[numTopics];

        // Loop over the tokens in the document, counting the current topic
        //  assignments.
        for (int position = 0; position < topics.getLength(); position++) {
            topicDistribution[topics.getIndexAtPosition(position)]++;
        }

        // Add the smoothing parameters and normalize
        double sum = 0.0;
        for (int topic = 0; topic < numTopics; topic++) {
            topicDistribution[topic] += gamma[modality] * alpha[modality][topic];
            sum += topicDistribution[topic];
        }

        // And normalize
        for (int topic = 0; topic < numTopics; topic++) {
            topicDistribution[topic] /= sum;
        }

        return topicDistribution;
    }

//    public void printDocumentTopics(File file) throws IOException {
//        PrintWriter out = new PrintWriter(new FileWriter(file));
//        printDocumentTopics(out);
//        out.close();
//    }
//    public void printDocumentTopics(PrintWriter out) {
//        printDocumentTopics(out, 0.0, -1);
//    }
    //    TODO save weights in DB (not needed any more as thay calculated on the fly)
    private double[] calcDiscrWeightAcrossTopicsPerModality(double[][] typeDiscrWeight) {
        // Calc Skew weight
        //skewOn == SkewType.LabelsOnly
        // The skew index of eachType
        //double[][] typeDiscrWeight = new double[numModalities][]; //<modality, type>
        double[] skewWeight = new double[numModalities];
        // The skew index of each Lbl Type
        //public double[] lblTypeSkewIndexes;
        //double [][] typeSkewIndexes = new 
        double skewSum = 0;
        int nonZeroSkewCnt = 1;

        for (Byte i = 0; i < numModalities; i++) {
            //typeDiscrWeight[i] = new double[numTypes[i]];

            for (int type = 0; type < numTypes[i]; type++) {

                int totalTypeCounts = 0;
                typeDiscrWeight[i][type] = 0;

                int[] targetCounts = typeTopicCounts[i][type];

                int index = 0;
                int count = 0;
                while (index < targetCounts.length) {
                    count = targetCounts[index];
                    typeDiscrWeight[i][type] += Math.pow((double) count, 2);
                    totalTypeCounts += count;
                    //currentTopic = currentTypeTopicCounts[index] & topicMask;
                    index++;
                }

                if (totalTypeCounts > 0) {
                    typeDiscrWeight[i][type] = typeDiscrWeight[i][type] / Math.pow((double) (totalTypeCounts), 2);
                }
                if (typeDiscrWeight[i][type] > 0) {
                    nonZeroSkewCnt++;
                    skewSum += typeDiscrWeight[i][type];
                }

            }

            skewWeight[i] = skewSum / (double) nonZeroSkewCnt;  // (double) 1 / (1 + skewSum / (double) nonZeroSkewCnt);
            //appendMetadata("Modality<" + i + "> Discr. Weight: " + formatter.format(skewWeight[i])); //LL for eachmodality

        }

        return skewWeight;

    }
//

    public double[] calcDiscrWeightWithinTopics(ArrayList<TreeSet<IDSorter>> topicSortedWords, boolean onDicrWeightTokens, int modality) {

        if (onDicrWeightTokens) {
            calcDiscrWeightAcrossTopicsPerModality(typeDiscrWeight);

        }

        double[] topicsSkewWeight = new double[numTopics];
        //ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords(0);

        for (int topic = 0; topic < numTopics; topic++) {

            topicsSkewWeight[topic] = 0.0;
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(topic);
            double totalTopicCnts = tokensPerTopic[modality][topic];

            if (onDicrWeightTokens) {
                totalTopicCnts = 0;
                for (IDSorter info : sortedWords) {
                    double tokenWeight = (onDicrWeightTokens ? typeDiscrWeight[modality][info.getID()] : 1);
                    double normCounts = tokenWeight * info.getWeight();
                    totalTopicCnts += normCounts;
                }
            }

            for (IDSorter info : sortedWords) {
                double tokenWeight = (onDicrWeightTokens ? typeDiscrWeight[modality][info.getID()] : 1);
                double probability = tokenWeight * info.getWeight() / totalTopicCnts;
                topicsSkewWeight[topic] += probability * probability;
            }
//                if (topicsSkewWeight[i][topic] == 0)
//                {
//                    logger.warning("calcDiscrWeightWithinTopics: zero weight for topic:"+topic);
//                }
        }

        return topicsSkewWeight;
    }

    public double[][] calcDiscrWeightWithinTopics(ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords, boolean onDicrWeightTokens) {

        double[][] topicsSkewWeight = new double[numModalities][];
        //ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords(0);
        for (Byte i = 0; i < numModalities; i++) {
            topicsSkewWeight[i] = calcDiscrWeightWithinTopics(topicSortedWords.get(i), onDicrWeightTokens, i);
        }

        return topicsSkewWeight;
    }

//    public double[][] calcTopicsSkewOnPubs(int[][] topicTypeCounts, ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords) {
//
//        //not implemented
//        return null;
//    }
    public void optimizeBeta() {
        // The histogram starts at count 0, so if all of the
        //  tokens of the most frequent type were assigned to one topic,
        //  we would need to store a maxTypeCount + 1 count.

        for (Byte m = 0; m < numModalities; m++) {
            double prevBetaSum = betaSum[m];
            int[] countHistogram = new int[maxTypeCount[m] + 1];

            //  Now count the number of type/topic pairs that have
            //  each number of tokens.
            for (int type = 0; type < numTypes[m]; type++) {

                int[] counts = typeTopicCounts[m][type];

                for (int topic = 0; topic < numTopics; topic++) {
                    int count = counts[topic];
                    if (count > 0) {
                        countHistogram[count]++;
                    }
                }
            }

            // Figure out how large we need to make the "observation lengths"
            //  histogram.
            int maxTopicSize = 0;
            for (int topic = 0; topic < numTopics; topic++) {
                if (tokensPerTopic[m][topic] > maxTopicSize) {
                    maxTopicSize = tokensPerTopic[m][topic];
                }
            }

            // Now allocate it and populate it.
            int[] topicSizeHistogram = new int[maxTopicSize + 1];
            for (int topic = 0; topic < numTopics; topic++) {
                topicSizeHistogram[tokensPerTopic[m][topic]]++;
            }

            try {
                betaSum[m] = Dirichlet.learnSymmetricConcentration(countHistogram,
                        topicSizeHistogram,
                        numTypes[m],
                        betaSum[m]);

                if (betaSum[m] < numTypes[m] * 0.0001) { //too sparse for this topic model (num of topics probably large for this modality).. prevent smoothing from going to zero 
                    logger.warn("Too sparse modality: set Beta to 0.0001");
                    beta[m] = 0.0001;
                    betaSum[m] = beta[m] * numTypes[m];

                } else if (Double.isNaN(betaSum[m])) {
                    //probably too sparse also??
                    //logger.warning("Dirichlet optimization has become unstable (NaN Value). Resetting to previous Beta");
                    if (beta[m] == 0.01) //initial beta... --> too sparse 
                    {
                        beta[m] = 0.0001;
                        betaSum[m] = beta[m] * numTypes[m];
                        logger.warn("Too sparse modality. Dirichlet optimization has failed. Set Beta to 0.001");
                    } else {
                        betaSum[m] = prevBetaSum;
                        beta[m] = betaSum[m] / numTypes[m];
                        logger.warn("Dirichlet optimization has become unstable (NaN Value). Resetting to previous Beta");
                    }
                } else {
                    beta[m] = betaSum[m] / numTypes[m];
                }
            } catch (RuntimeException e) {
                // Dirichlet optimization has become unstable. This is known to happen for very small corpora (~5 docs).
                logger.warn("Dirichlet optimization has become unstable:" + e.getMessage() + ". Resetting to previous Beta");
                betaSum[m] = prevBetaSum;
                beta[m] = betaSum[m] / numTypes[m];

            }
            //TODO: copy/update trees in threads
            logger.info("[beta[" + m + "]: " + formatter.format(beta[m]) + "] ");
            // Now publish the new value
            // for (int thread = 0; thread < numThreads; thread++) {
            //     runnables[thread].resetBeta(beta[0], betaSum[0]);
            // }
        }
    }

    private void optimizeGamma() {

        // hyperparameters for DP and Dirichlet samplers
        // Teh+06: Docs: (1, 1), M1-3: (0.1, 0.1); HMM: (1, 1)
        double aalpha = 5;
        double balpha = 0.1;
        //double abeta = 0.1;
        //double bbeta = 0.1;
        // Teh+06: Docs: (1, 0.1), M1-3: (5, 0.1), HMM: (1, 1)
        double agamma = 5;
        double bgamma = 0.1;
        // number of samples for parameter samplers
        int R = 10;

        //root level DP for all modalities (overall measure) 
        for (int r = 0; r < R; r++) {

            // gamma[0]: root level (Escobar+West95) with n = T
            // (14)
            double eta = samp.randBeta(gammaRoot + 1, rootTablesCnt);
            double bloge = bgamma - log(eta);
            // (13')
            double pie = 1. / (1. + (rootTablesCnt * bloge / (agamma + numTopics - 1)));
            // (13)
            int u = samp.randBernoulli(pie);
            gammaRoot = samp.randGamma(agamma + numTopics - 1 + u, 1. / bloge);
        }

        //second level DP per modality (Global measure per modality) 
        for (Byte m = 0; m < numModalities; m++) {

            for (int r = 0; r < R; r++) {
                double prevGamma = gamma[m];
                // gamma[0]: root level (Escobar+West95) with n = T
                // (14)
                double eta = samp.randBeta(gammaView[m] + 1, tablesCnt[m]);
                double bloge = bgamma - log(eta);
                // (13')
                double pie = 1. / (1. + (tablesCnt[m] * bloge / (agamma + numTopics - 1)));
                // (13)
                int u = samp.randBernoulli(pie);
                gammaView[m] = samp.randGamma(agamma + numTopics - 1 + u, 1. / bloge);

                // document level (Teh+06) measure
                double qs = 0;
                double qw = 0;
                for (int j = 0; j < docLengthCounts[m].length; j++) {
                    for (int i = 0; i < docLengthCounts[m][j]; i++) {
                        // (49) (corrected)
                        qs += samp.randBernoulli(j / (j + gamma[m]));
                        // (48)
                        qw += log(samp.randBeta(gamma[m] + 1, j));
                    }
                }
                // (47)
                gamma[m] = samp.randGamma(aalpha + tablesCnt[m] - qs, 1. / (balpha - qw));
                if (gamma[m] == 0) {
                    gamma[m] = prevGamma;
                    logger.info("Warning: Gamma optimization become unstable for modality[" + m + "]. Return to previous Gamma! ");
                }
                //  }
            }
            logger.info("GammaRoot: " + gammaRoot);
            logger.info("GammaView[" + m + "]: " + gammaView[m]);
            //for (byte m = 0; m < numModalities; m++) {
            logger.info("Gamma[" + m + "]: " + gamma[m]);
            //}
        }

    }

    private void optimizeDP() {

        double[][] mk = new double[numModalities][numTopics + 1];

        double[] mk_root = new double[numTopics + 1];

        Arrays.fill(tablesCnt, 0);
        Arrays.fill(mk_root, 0);

        for (int t = 0; t < numTopics; t++) {
            inActiveTopicIndex.add(t); //inActive by default and activate if found 
        }

        // view tables simulation
        for (byte m = 0; m < numModalities; m++) {
            for (int t = 0; t < numTopics; t++) {

                //int k = kactive.get(kk);
                for (int i = 0; i < topicDocCounts[m][t].length; i++) {
                    //for (int j = 0; j < numDocuments; j++) {

                    if (topicDocCounts[m][t][i] > 0 && i > 1) {
                        inActiveTopicIndex.remove(new Integer(t));  //..remove(t);
                        //sample number of tables
                        // number of tables a CRP(alpha tau) produces for nmk items
                        //TODO: See if  using the "minimal path" assumption  to reduce bookkeeping gives the same results. 
                        //Huge Memory consumption due to  topicDocCounts (* NumThreads), and striling number of first kind allss double[][] 
                        //Also 2x slower than the parametric version due to UpdateAlphaAndSmoothing

                        int curTbls = 0;
                        try {
                            curTbls = Samplers.randAntoniak(gamma[m] * alpha[m][t], i);

                        } catch (Exception e) {
                            curTbls = 1;
                        }

                        mk[m][t] += (topicDocCounts[m][t][i] * curTbls);
                        //mk[m][t] += 1;//direct minimal path assignment Samplers.randAntoniak(gamma[0][m] * alpha[m].get(t),  tokensPerTopic[m].get(t));
                        // nmk[m].get(k));
                    } else if (topicDocCounts[m][t][i] > 0 && i == 1) //nmk[m].get(k) = 0 or 1
                    {
                        inActiveTopicIndex.remove(new Integer(t));
                        mk[m][t] += topicDocCounts[m][t][i];
                    }
                }
            }
        }

        //root tables simulation
        for (int t = 0; t < numTopics; t++) {

            //int k = kactive.get(kk);
            for (byte m = 0; m < numModalities; m++) {
                //for (int j = 0; j < numDocuments; j++) {

                if (mk[m][t] > 1) {

                    int curTbls = 0;
                    try {
                        curTbls = Samplers.randAntoniak(gammaRoot, (int) Math.ceil(mk[m][t]));

                    } catch (Exception e) {
                        curTbls = 1;
                    }

                    mk_root[t] += curTbls;
                    //mk[m][t] += 1;//direct minimal path assignment Samplers.randAntoniak(gamma[0][m] * alpha[m].get(t),  tokensPerTopic[m].get(t));
                    // nmk[m].get(k));
                } else if (mk[m][t] == 1) //nmk[m].get(k) = 0 or 1
                {

                    mk_root[t] += 1;
                }
            }
        }

        double[] v = new double[numTopics + 1];
        Arrays.fill(v, 0);
        //alphaSum[m] = 0;
        mk_root[numTopics] = gammaRoot;
        rootTablesCnt = Vectors.sum(mk_root);

        byte numSamples = 10;
        for (int i = 0; i < numSamples; i++) {
            double[] tt = sampleDirichlet(mk_root);
            // On non parametric with new topic we would have numTopics+1 topics for (int kk = 0; kk <= numTopics; kk++) {
            for (int kk = 0; kk <= numTopics; kk++) {
                //int k = kactive.get(kk);
                double sampleAlpha = tt[kk] / (double) numSamples;
                v[kk] += sampleAlpha;
                //alphaSum[m] += sampleAlpha;
                //tau.set(k, tt[kk]);
            }
        }

        if (!inActiveTopicIndex.isEmpty()) {
            String empty = "";

            for (int i = 0; i < inActiveTopicIndex.size(); i++) {
                empty += formatter.format(inActiveTopicIndex.get(i)) + " ";
            }
            logger.info("Inactive Topics: " + empty);
        }

        for (byte m = 0; m < numModalities; m++) {
            //for (byte m = 0; m < numModalities; m++) {

            for (int t = 0; t < numTopics; t++) {
                mk[m][t] += v[t] * gammaRoot;
            }
            Arrays.fill(alpha[m], 0);
            alphaSum[m] = 0;
            mk[m][numTopics] = gammaView[m] + v[numTopics] * gammaRoot;
            tablesCnt[m] = Vectors.sum(mk[m]);

            numSamples = 10;
            for (int i = 0; i < numSamples; i++) {
                double[] tt = sampleDirichlet(mk[m]);
                // On non parametric with new topic we would have numTopics+1 topics for (int kk = 0; kk <= numTopics; kk++) {
                for (int kk = 0; kk <= numTopics; kk++) {
                    //int k = kactive.get(kk);
                    double sampleAlpha = tt[kk] / (double) numSamples;
                    alpha[m][kk] += sampleAlpha;
                    alphaSum[m] += sampleAlpha;
                    //tau.set(k, tt[kk]);
                }
            }

            logger.info("AlphaSum[" + m + "]: " + alphaSum[m]);
            //for (byte m = 0; m < numModalities; m++) {
            String alphaStr = "";
            for (int topic = 0; topic < numTopics; topic += 10) {
                alphaStr += topic + ":" + formatter.format(alpha[m][topic]) + " ";
            }

            logger.info("[Alpha[" + m + "]: [" + alphaStr + "] ");
        }

//            if (alpha[m].size() < numTopics + 1) {
//                alpha[m].add(tt[numTopics]);
//            } else {
//                alpha[m].set(numTopics, tt[numTopics]);
//            }
        //tau.set(K, tt[K]);
        //}
        //Recalc trees
    }

    private double[] sampleDirichlet(double[] p) {
        double magnitude = 1;
        double[] partition;

        magnitude = 0;
        partition = new double[p.length];

        // Add up the total
        for (int i = 0; i < p.length; i++) {
            magnitude += p[i];
        }

        for (int i = 0; i < p.length; i++) {
            partition[i] = p[i] / magnitude;
        }

        double distribution[] = new double[partition.length];

//		For each dimension, draw a sample from Gamma(mp_i, 1)
        double sum = 0;
        for (int i = 0; i < distribution.length; i++) {

            if (partition[i] * magnitude > 0) {
                distribution[i] = random.nextGamma(partition[i] * magnitude, 1);
                if (distribution[i] <= 0) {
                    distribution[i] = 0.0001;
                }
            } else {
                distribution[i] = 0.0001;
            }
            sum += distribution[i];
        }

//		Normalize
        for (int i = 0; i < distribution.length; i++) {
            distribution[i] /= sum;
        }

        return distribution;
    }

//    private void recalcTrees() {
//        //recalc trees
//        double[] temp = new double[numTopics];
//        for (Byte m = 0; m < numModalities; m++) {
//            for (int w = 0; w < numTypes[m]; ++w) {
//
//                int[] currentTypeTopicCounts = typeTopicCounts[m][w];
//                for (int currentTopic = 0; currentTopic < numTopics; currentTopic++) {
//
////                temp[currentTopic] = (currentTypeTopicCounts[currentTopic] + beta[0]) * alpha[currentTopic] / (tokensPerTopic[currentTopic] + betaSum);
//                    if (useCycleProposals) {
//                        temp[currentTopic] = (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]); //with cycle proposal
//                    } else {
//                        temp[currentTopic] = gamma[m] * alpha[m][currentTopic] * (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);
//                    }
//
//                }
//
//                trees[m][w].constructTree(temp);
//
//                //reset temp
//                Arrays.fill(temp, 0);
//            }
//        }
//
//    }
    private FastQMVWVParallelTopicModel initFromPreviousRun(String stateFile) {
        //currentAlphabet.lookupIndex(singular)
        try {
            FastQMVWVParallelTopicModel prevTopicModel = FastQMVWVParallelTopicModel.read(new File(stateFile));

            //build trees per type
            double[] temp = new double[prevTopicModel.numTopics];
            Arrays.fill(temp, 0);
            for (Byte m = 0; m < prevTopicModel.numModalities; m++) {
                for (int w = 0; w < prevTopicModel.numTypes[m]; ++w) {

                    int[] currentTypeTopicCounts = prevTopicModel.typeTopicCounts[m][w];
                    for (int currentTopic = 0; currentTopic < prevTopicModel.numTopics; currentTopic++) {
                        temp[currentTopic] = (currentTypeTopicCounts[currentTopic] + prevTopicModel.beta[m]) / (prevTopicModel.tokensPerTopic[m][currentTopic] + prevTopicModel.betaSum[m]);

                    }

                    //trees[w].init(numTopics);
                    prevTopicModel.trees[m][w] = new FTree(temp);

                    //reset temp
                    Arrays.fill(temp, 0);

                }

            }
            return prevTopicModel;
        } catch (Exception e) {
            logger.error("File input error:");
            return null;
        }

    }

    private void buildFTrees(boolean initTree) {

        double[] temp = new double[numTopics];
        Arrays.fill(temp, 0);
        for (Byte m = 0; m < numModalities; m++) {
            for (int w = 0; w < numTypes[m]; ++w) {

                int[] currentTypeTopicCounts = typeTopicCounts[m][w];
                for (int currentTopic = 0; currentTopic < numTopics; currentTopic++) {

                    if (!inActiveTopicIndex.isEmpty() && inActiveTopicIndex.contains(currentTopic)) {
                        temp[currentTopic] = 0;
                    } else {
                        double p_wt = (useVectorsLambda != 0 && m == 0)
                                ? (useVectorsLambda * (expDotProductValues[currentTopic][w] / sumExpValues[currentTopic]) + (1 - useVectorsLambda) * ((currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m])))
                                : (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);

                        temp[currentTopic] = gamma[m] * alpha[m][currentTopic] * p_wt;
                    }

                }
                if (initTree) {
                    //trees[w].init(numTopics);
                    trees[m][w] = new FTree(temp);
                } else {
                    trees[m][w].constructTree(temp);
                }
                //reset temp
                Arrays.fill(temp, 0);

            }

            docSmoothingOnlyMass[m] = 0;
        }

    }

    public void optimizeP(boolean appendMetadata) {

//          for (int thread = 0; thread < numThreads; thread++) {
//              runnables[thread].getPDistr_Mean();
//          }
//we consider beta known = 1 --> a=(inverse digamma) [lnGx-lnG(1-x)+y(b)]
        // --> a = - 1 / (1/N (Sum(lnXi))), i=1..N , where Xi = mean (pDistr_Mean)
//statistics for p optimization
        double[][][] pDistr_Mean = new double[numModalities][numModalities][data.size()];; // modalities correlation distribution accross documents (used in a, b beta params optimization)

        for (int docCnt = 0;
                docCnt < data.size();
                docCnt++) {
            MixTopicModelTopicAssignment doc = data.get(docCnt);
            int[][] localTopicCounts = new int[numModalities][numTopics];
            int[] oneDocTopics;
            FeatureSequence tokenSequence;
            int[] docLength = new int[numModalities];

            TreeMap<Integer, Byte> sortedViews
                    = new TreeMap<Integer, Byte>();

            for (byte m = 0; m < numModalities; m++) {

                docLength[m] = 0;
                if (doc.Assignments[m] != null) {
                    //TODO can I order by tokens/topics??
                    oneDocTopics = doc.Assignments[m].topicSequence.getFeatures();

                    //System.arraycopy(oneDocTopics[m], 0, doc.Assignments[m].topicSequence.getFeatures(), 0, doc.Assignments[m].topicSequence.getFeatures().length-1);
                    tokenSequence = ((FeatureSequence) doc.Assignments[m].instance.getData());
                    docLength[m] = tokenSequence.getLength(); //size is the same??

                    //		populate topic counts
                    for (int position = 0; position < docLength[m]; position++) {
                        if (oneDocTopics[position] == UNASSIGNED_TOPIC) {
                            System.err.println(" Init Sampling UNASSIGNED_TOPIC");
                            continue;
                        }
                        localTopicCounts[m][oneDocTopics[position]]++; //, localTopicCounts[m][oneDocTopics[m][position]] + 1);

                    }
                }
                sortedViews.put(new Integer(docLength[m]), new Byte(m));
            }

            Iterator iter = sortedViews.descendingMap().entrySet().iterator();
            ArrayList<Byte> previousViews = new ArrayList<Byte>();

            previousViews.add((Byte) ((Map.Entry) iter.next()).getValue());

            // Traversing map. Note that the traversal
            // produced sorted (by keys) output .
            while (iter.hasNext()) {
                Map.Entry val = (Map.Entry) iter.next();

                byte m = (Byte) val.getValue();

                if (doc.Assignments[m] != null) {

                    oneDocTopics = doc.Assignments[m].topicSequence.getFeatures();

                    tokenSequence = ((FeatureSequence) doc.Assignments[m].instance.getData());

                    for (int position = 0; position < tokenSequence.getLength(); position++) {
                        if (oneDocTopics[position] == UNASSIGNED_TOPIC) {
                            System.err.println(" Init Sampling UNASSIGNED_TOPIC");
                            continue;
                        }
//TODO: we should sort modalities according to length[m]

                        for (byte i : previousViews) {

                            pDistr_Mean[m][i][docCnt] += (localTopicCounts[i][oneDocTopics[position]] > 0 ? 1.0 : 0d) / (double) docLength[m];
                            pDistr_Mean[i][m][docCnt] = pDistr_Mean[m][i][docCnt];
                            //pDistr_Var[m][i][docCnt]+= localTopicCounts[i][newTopic]/docLength[m];
                        }

                    }
                }
                previousViews.add(m);

            }

        }

        for (Byte m = 0; m < numModalities; m++) {
            pMean[m][m] = 1;

            for (Byte i = (byte) (m + 1); i < numModalities; i++) {
                //optimize based on mean & variance
                double sum = 0;
                for (int j = 0; j < pDistr_Mean[m][i].length; j++) {
                    sum += pDistr_Mean[m][i][j];
                }

                pMean[m][i] = sum / (Math.min(totalDocsPerModality[m], totalDocsPerModality[i]));
                pMean[i][m] = pMean[m][i];

                //double var = 2 * (1 - mean);
                double a = pMean[m][i] == 1 ? 5000 : -1.0 / Math.log(pMean[m][i]);
                double b = 1;

                logger.info("[p:" + m + "_" + i + " mean:" + pMean[m][i] + " a:" + a + " b:" + b + "] ");
                if (appendMetadata) { //do it only once
                    appendMetadata("[p:" + m + "_" + i + " mean:" + pMean[m][i] + " a:" + a + " b:" + b + "] ");
                }

                p_a[m][i] = Math.min(a, 100);//a=100--> almost p=99%
                p_a[i][m] = Math.min(a, 100);
                p_b[m][i] = b;
                p_b[i][m] = b;

            }
        }

        // Now publish the new value
//        for (int thread = 0; thread < numThreads; thread++) {
//            runnables[thread].resetP_a(p_a);
//            runnables[thread].resetP_a(p_b);
//        }
    }

    public void printDocumentTopics(PrintWriter out, double threshold, int max, String SQLLiteDB, String experimentId, String batchId) {
        if (out != null) {
            out.print("#doc name topic proportion ...\n");
        }
        int[] docLen = new int[numModalities];

        int[][] topicCounts = new int[numModalities][numTopics];

        IDSorter[] sortedTopics = new IDSorter[numTopics];
        for (int topic = 0; topic < numTopics; topic++) {
            // Initialize the sorters with dummy values
            sortedTopics[topic] = new IDSorter(topic, topic);
        }

        if (max < 0 || max > numTopics) {
            max = numTopics;
        }

        double[] skewWeight = calcDiscrWeightAcrossTopicsPerModality(typeDiscrWeight);
        for (byte m = 0; m < numModalities; m++) {
            appendMetadata("Modality<" + m + "> Discr. Weight: " + formatter.format(skewWeight[m])); //LL for eachmodality
        }

        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();
                // statement.executeUpdate("drop table if exists PubTopic");
                //statement.executeUpdate("create table if not exists PubTopic (PubId nvarchar(50), TopicId Integer, Weight Double , BatchId Text, ExperimentId nvarchar(50)) ");
                //statement.executeUpdate(String.format("Delete from PubTopic where  ExperimentId = '%s'", experimentId));
            }
            PreparedStatement bulkInsert = null;
            String sql = "insert into PubTopic values(?,?,?,?,? );";

            try {
                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (int doc = 0; doc < data.size(); doc++) {
                    int cntEnd = numModalities;
                    StringBuilder builder = new StringBuilder();
                    builder.append(doc);
                    builder.append("\t");

                    String docId = "no-name";

                    docId = data.get(doc).EntityId.toString();

                    builder.append(docId);
                    builder.append("\t");

                    for (Byte m = 0; m < cntEnd; m++) {
                        if (data.get(doc).Assignments[m] != null) {
                            Arrays.fill(topicCounts[m], 0);
                            LabelSequence topicSequence = (LabelSequence) data.get(doc).Assignments[m].topicSequence;

                            int[] currentDocTopics = topicSequence.getFeatures();

                            docLen[m] = data.get(doc).Assignments[m].topicSequence.getLength();// currentDocTopics.length;

                            // Count up the tokens
                            for (int token = 0; token < docLen[m]; token++) {
                                topicCounts[m][currentDocTopics[token]]++;
                            }
                        }
                    }

                    // And normalize
                    for (int topic = 0; topic < numTopics; topic++) {
                        double topicProportion = 0;
                        double normalizeSum = 0;
                        for (Byte m = 0; m < cntEnd; m++) {
                            //Omiros: TODO: I should reweight each modality's contribution in the proportion of the document based on its discrimination power (skew index)
                            //topicProportion += (m == 0 ? 1 : skewWeight[m]) * pMean[0][m] * ((double) topicCounts[m][topic] + (double) gamma[m] * alpha[m][topic]) / (docLen[m] + alphaSum[m]);
                            //normalizeSum += (m == 0 ? 1 : skewWeight[m]) * pMean[0][m];

                            topicProportion += (m == 0 ? 1 : skewWeight[m]) * pMean[0][m] * ((double) topicCounts[m][topic] + (double) gamma[m] * alpha[m][topic]) / (docLen[m] + (double) gamma[m] * alphaSum[m]);
                            normalizeSum += (m == 0 ? 1 : skewWeight[m]) * pMean[0][m];
                        }
                        sortedTopics[topic].set(topic, (topicProportion / normalizeSum));

                    }

                    Arrays.sort(sortedTopics);

//      statement.executeUpdate("insert into person values(1, 'leo')");
//      statement.executeUpdate("insert into person values(2, 'yui')");
//      ResultSet rs = statement.executeQuery("select * from person");
                    for (int i = 0; i < max; i++) {
                        if (sortedTopics[i].getWeight() < threshold) {
                            break;
                        }

                        builder.append(sortedTopics[i].getID() + "\t"
                                + sortedTopics[i].getWeight() + "\t");
                        if (out != null) {
                            out.println(builder);
                        }

                        if (!SQLLiteDB.isEmpty()) {
                            //  sql += String.format(Locale.ENGLISH, "insert into PubTopic values('%s',%d,%.4f,'%s' );", docId, sortedTopics[i].getID(), sortedTopics[i].getWeight(), experimentId);
                            bulkInsert.setString(1, docId);
                            bulkInsert.setInt(2, sortedTopics[i].getID());
                            bulkInsert.setDouble(3, (double) Math.round(sortedTopics[i].getWeight() * 10000) / 10000);
                            bulkInsert.setString(4, batchId);
                            bulkInsert.setString(5, experimentId);
                            bulkInsert.executeUpdate();

                        }

                    }

//                    if ((doc / 10) * 10 == doc && !sql.isEmpty()) {
//                        statement.executeUpdate(sql);
//                        sql = "";
//                    }
                }
                if (!SQLLiteDB.isEmpty()) {
                    connection.commit();
                }
//                if (!sql.isEmpty()) {
//                    statement.executeUpdate(sql);
//                }
//

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        logger.error(e.getMessage());
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error(excep.getMessage());
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
            logger.error(e.getMessage());
            System.err.println(e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e.getMessage());
                System.err.println(e);
            }

        }
    }

    public void CreateTables(String SQLLiteDB, String experimentId) {
        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();
                // statement.executeUpdate("drop table if exists PubTopic");
                //statement.executeUpdate("create table if not exists PubTopic (PubId nvarchar(50), TopicId Integer, Weight Double , BatchId Text, ExperimentId nvarchar(50)) ");
                statement.executeUpdate(String.format("Delete from PubTopic where  ExperimentId = '%s'", experimentId));

                //statement.executeUpdate("create table if not exists Experiment (ExperimentId nvarchar(50), Description nvarchar(200), Metadata nvarchar(500), InitialSimilarity Double, PhraseBoost Integer) ");
                String deleteSQL = String.format("Delete from Experiment where  ExperimentId = '%s'", experimentId);
                statement.executeUpdate(deleteSQL);

                //statement.executeUpdate("create table if not exists TopicDetails (TopicId integer, ItemType integer,  Weight double, TotalTokens int, BatchId TEXT,ExperimentId nvarchar(50)) ");
                deleteSQL = String.format("Delete from TopicDetails where  ExperimentId = '%s'", experimentId);
                statement.executeUpdate(deleteSQL);

                deleteSQL = String.format("Delete from TopicDescription where  ExperimentId = '%s'", experimentId);
                statement.executeUpdate(deleteSQL);

                //statement.executeUpdate("create table if not exists TopicAnalysis (TopicId integer, ItemType integer, Item nvarchar(100), Counts double,  BatchId TEXT, ExperimentId nvarchar(50)) ");
                deleteSQL = String.format("Delete from TopicAnalysis where  ExperimentId = '%s'", experimentId);
                statement.executeUpdate(deleteSQL);

                //statement.executeUpdate("create table if not exists PubTopic (PubId nvarchar(50), TopicId Integer, Weight Double , BatchId Text, ExperimentId nvarchar(50)) ");
                statement.executeUpdate(String.format("Delete from PubTopic where  ExperimentId = '%s'", experimentId));

                //statement.executeUpdate("create table if not exists ExpDiagnostics (ExperimentId text, BatchId text, EntityId text, EntityType int, ScoreName text, Score double )");
                deleteSQL = String.format("Delete from ExpDiagnostics where  ExperimentId = '%s'", experimentId);
                statement.executeUpdate(deleteSQL);
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

    /**
     * @param out	A print writer
     * @param threshold Only print topics with proportion greater than this
     * number
     * @param max	Print no more than this many topics
     */
//    public void printDocumentTopics(PrintWriter out, double threshold, int max) {
//        out.print("#doc name topic proportion ...\n");
//        int docLen;
//        int[] topicCounts = new int[numTopics];
//
//        IDSorter[] sortedTopics = new IDSorter[numTopics];
//        for (int topic = 0; topic < numTopics; topic++) {
//            // Initialize the sorters with dummy values
//            sortedTopics[topic] = new IDSorter(topic, topic);
//        }
//
//        if (max < 0 || max > numTopics) {
//            max = numTopics;
//        }
//
//        for (int doc = 0; doc < data.size(); doc++) {
//            LabelSequence topicSequence = (LabelSequence) data.get(doc).topicSequence;
//            int[] currentDocTopics = topicSequence.getFeatures();
//
//            StringBuilder builder = new StringBuilder();
//
//            builder.append(doc);
//            builder.append("\t");
//
//            if (data.get(doc).instance.getName() != null) {
//                builder.append(data.get(doc).instance.getName());
//            } else {
//                builder.append("no-name");
//            }
//
//            builder.append("\t");
//            docLen = currentDocTopics.length;
//
//            // Count up the tokens
//            for (int token = 0; token < docLen; token++) {
//                topicCounts[currentDocTopics[token]]++;
//            }
//
//            // And normalize
//            for (int topic = 0; topic < numTopics; topic++) {
//                sortedTopics[topic].set(topic, (gamma[0] * alpha[topic] + topicCounts[topic]) / (docLen + alphaSum[0]));
//            }
//
//            Arrays.sort(sortedTopics);
//
//            for (int i = 0; i < max; i++) {
//                if (sortedTopics[i].getWeight() < threshold) {
//                    break;
//                }
//
//                builder.append(sortedTopics[i].getID() + "\t"
//                        + sortedTopics[i].getWeight() + "\t");
//            }
//            out.println(builder);
//
//            Arrays.fill(topicCounts, 0);
//        }
//
//    }
//
//    public double[][] getSubCorpusTopicWords(boolean[] documentMask, boolean normalized, boolean smoothed) {
//        double[][] result = new double[numTopics][numTypes];
//        int[] subCorpusTokensPerTopic = new int[numTopics];
//
//        for (int doc = 0; doc < data.size(); doc++) {
//            if (documentMask[doc]) {
//                int[] words = ((FeatureSequence) data.get(doc).instance.getData()).getFeatures();
//                int[] topics = data.get(doc).topicSequence.getFeatures();
//                for (int position = 0; position < topics.length; position++) {
//                    result[topics[position]][words[position]]++;
//                    subCorpusTokensPerTopic[topics[position]]++;
//                }
//            }
//        }
//
//        if (smoothed) {
//            for (int topic = 0; topic < numTopics; topic++) {
//                for (int type = 0; type < numTypes; type++) {
//                    result[topic][type] += beta[0];
//                }
//            }
//        }
//
//        if (normalized) {
//            double[] topicNormalizers = new double[numTopics];
//            if (smoothed) {
//                for (int topic = 0; topic < numTopics; topic++) {
//                    topicNormalizers[topic] = 1.0 / (subCorpusTokensPerTopic[topic] + numTypes * beta[0]);
//                }
//            } else {
//                for (int topic = 0; topic < numTopics; topic++) {
//                    topicNormalizers[topic] = 1.0 / subCorpusTokensPerTopic[topic];
//                }
//            }
//
//            for (int topic = 0; topic < numTopics; topic++) {
//                for (int type = 0; type < numTypes; type++) {
//                    result[topic][type] *= topicNormalizers[topic];
//                }
//            }
//        }
//
//        return result;
//    }
//
//    public double[][] getTopicWords(boolean normalized, boolean smoothed) {
//        double[][] result = new double[numTopics][numTypes];
//
//        for (int type = 0; type < numTypes; type++) {
//            int[] topicCounts = typeTopicCounts[type];
//
//            int index = 0;
//            while (index < topicCounts.length
//                    && topicCounts[index] > 0) {
//
//                result[index][type] += topicCounts[index];
//
//                index++;
//            }
//        }
//
//        if (smoothed) {
//            for (int topic = 0; topic < numTopics; topic++) {
//                for (int type = 0; type < numTypes; type++) {
//                    result[topic][type] += beta[0];
//                }
//            }
//        }
//
//        if (normalized) {
//            double[] topicNormalizers = new double[numTopics];
//            if (smoothed) {
//                for (int topic = 0; topic < numTopics; topic++) {
//                    topicNormalizers[topic] = 1.0 / (tokensPerTopic[topic] + numTypes * beta[0]);
//                }
//            } else {
//                for (int topic = 0; topic < numTopics; topic++) {
//                    topicNormalizers[topic] = 1.0 / tokensPerTopic[topic];
//                }
//            }
//
//            for (int topic = 0; topic < numTopics; topic++) {
//                for (int type = 0; type < numTypes; type++) {
//                    result[topic][type] *= topicNormalizers[topic];
//                }
//            }
//        }
//
//        return result;
//    }
//
//    public double[][] getDocumentTopics(boolean normalized, boolean smoothed) {
//        double[][] result = new double[data.size()][numTopics];
//
//        for (int doc = 0; doc < data.size(); doc++) {
//            int[] topics = data.get(doc).topicSequence.getFeatures();
//            for (int position = 0; position < topics.length; position++) {
//                result[doc][topics[position]]++;
//            }
//
//            if (smoothed) {
//                for (int topic = 0; topic < numTopics; topic++) {
//                    result[doc][topic] += gamma[0] * alpha[topic];
//                }
//            }
//
//            if (normalized) {
//                double sum = 0.0;
//                for (int topic = 0; topic < numTopics; topic++) {
//                    sum += result[doc][topic];
//                }
//                double normalizer = 1.0 / sum;
//                for (int topic = 0; topic < numTopics; topic++) {
//                    result[doc][topic] *= normalizer;
//                }
//            }
//        }
//
//        return result;
//    }
//
//    public ArrayList<TreeSet<IDSorter>> getTopicDocuments(double smoothing) {
//        ArrayList<TreeSet<IDSorter>> topicSortedDocuments = new ArrayList<TreeSet<IDSorter>>(numTopics);
//
//        // Initialize the tree sets
//        for (int topic = 0; topic < numTopics; topic++) {
//            topicSortedDocuments.add(new TreeSet<IDSorter>());
//        }
//
//        int[] topicCounts = new int[numTopics];
//
//        for (int doc = 0; doc < data.size(); doc++) {
//            int[] topics = data.get(doc).topicSequence.getFeatures();
//            for (int position = 0; position < topics.length; position++) {
//                topicCounts[topics[position]]++;
//            }
//
//            for (int topic = 0; topic < numTopics; topic++) {
//                topicSortedDocuments.get(topic).add(new IDSorter(doc, (topicCounts[topic] + smoothing) / (topics.length + numTopics * smoothing)));
//                topicCounts[topic] = 0;
//            }
//        }
//
//        return topicSortedDocuments;
//    }
//
//    public void printTopicDocuments(PrintWriter out) {
//        printTopicDocuments(out, 100);
//    }
//
//    /**
//     * @param out	A print writer
//     * @param count Print this number of top documents
//     */
//    public void printTopicDocuments(PrintWriter out, int max) {
//        out.println("#topic doc name proportion ...");
//
//        ArrayList<TreeSet<IDSorter>> topicSortedDocuments = getTopicDocuments(10.0);
//
//        for (int topic = 0; topic < numTopics; topic++) {
//            TreeSet<IDSorter> sortedDocuments = topicSortedDocuments.get(topic);
//
//            int i = 0;
//            for (IDSorter sorter : sortedDocuments) {
//                if (i == max) {
//                    break;
//                }
//
//                int doc = sorter.getID();
//                double proportion = sorter.getWeight();
//                String name = (String) data.get(doc).instance.getName();
//                if (name == null) {
//                    name = "no-name";
//                }
//                out.format("%d %d %s %f\n", topic, doc, name, proportion);
//
//                i++;
//            }
//        }
//    }
    public void printState(File f) throws IOException {
        PrintStream out
                = new PrintStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(f))));
        printState(out);
        out.close();
    }

    public void printState(PrintStream out) {

        out.println("#doc source pos typeindex type topic");
        out.print("#alpha : ");
        for (Byte m = 0; m < numModalities; m++) {
            out.println("modality:" + m);
            for (int topic = 0; topic < numTopics; topic++) {
                out.print(gamma[m] * alpha[m][topic] + " ");
            }
        }
        out.println();
        out.println("#beta[0] : " + beta[0]);

        for (int doc = 0; doc < data.size(); doc++) {
            for (Byte m = 0; m < numModalities; m++) {
                FeatureSequence tokenSequence = (FeatureSequence) data.get(doc).Assignments[m].instance.getData();
                LabelSequence topicSequence = (LabelSequence) data.get(doc).Assignments[m].topicSequence;

                String source = "NA";
                if (data.get(doc).Assignments[m].instance.getSource() != null) {
                    source = data.get(doc).Assignments[m].instance.getSource().toString();
                }

                Formatter output = new Formatter(new StringBuilder(), Locale.US);

                for (int pi = 0; pi < topicSequence.getLength(); pi++) {
                    int type = tokenSequence.getIndexAtPosition(pi);
                    int topic = topicSequence.getIndexAtPosition(pi);

                    output.format("%d %s %d %d %s %d\n", doc, source, pi, type, alphabet[m].lookupObject(type), topic);

                    /*
                     out.print(doc); out.print(' ');
                     out.print(source); out.print(' '); 
                     out.print(pi); out.print(' ');
                     out.print(type); out.print(' ');
                     out.print(alphabet.lookupObject(type)); out.print(' ');
                     out.print(topic); out.println();
                     */
                }

                out.print(output);
            }
        }
    }

    public double[] modelLogLikelihood() {
        double[] logLikelihood = new double[numModalities];
        Arrays.fill(logLikelihood, 0);
        //int nonZeroTopics;

        // The likelihood of the model is a combination of a 
        // Dirichlet-multinomial for the words in each topic
        // and a Dirichlet-multinomial for the topics in each
        // document.
        // The likelihood function of a dirichlet multinomial is
        //	 Gamma( sum_i alpha_i )	 prod_i Gamma( alpha_i + N_i )
        //	prod_i Gamma( alpha_i )	  Gamma( sum_i (alpha_i + N_i) )
        // So the log likelihood is 
        //	logGamma ( sum_i alpha_i ) - logGamma ( sum_i (alpha_i + N_i) ) + 
        //	 sum_i [ logGamma( alpha_i + N_i) - logGamma( alpha_i ) ]
        // Do the documents first
        int[] topicCounts = new int[numTopics];
        double[] topicLogGammas = new double[numTopics];
        int[] docTopics;
        for (Byte m = 0; m < numModalities; m++) {
            for (int topic = 0; topic < numTopics; topic++) {
                topicLogGammas[topic] = Dirichlet.logGammaStirling(gamma[m] * alpha[m][topic]);
            }

            int modalityCnt = 0;
            for (int doc = 0; doc < data.size(); doc++) {
                if (data.get(doc).Assignments[m] != null) {
                    LabelSequence topicSequence = (LabelSequence) data.get(doc).Assignments[m].topicSequence;

                    docTopics = topicSequence.getFeatures();
                    if (docTopics.length > 0) {
                        for (int token = 0; token < docTopics.length; token++) {
                            topicCounts[docTopics[token]]++;
                        }

                        for (int topic = 0; topic < numTopics; topic++) {
                            if (topicCounts[topic] > 0) {
                                logLikelihood[m] += (Dirichlet.logGammaStirling(gamma[m] * alpha[m][topic] + topicCounts[topic])
                                        - topicLogGammas[topic]);
                            }
                        }

                        // subtract the (count + parameter) sum term
                        logLikelihood[m] -= Dirichlet.logGammaStirling((double) gamma[m] * alphaSum[m] + docTopics.length);
                        modalityCnt++;
                    }
                    Arrays.fill(topicCounts, 0);
                }
            }

            // add the parameter sum term
            logLikelihood[m] += modalityCnt * Dirichlet.logGammaStirling((double) gamma[m] * alphaSum[m]);

            if (Double.isNaN(logLikelihood[m])) {
                logger.warn("NaN in log likelihood level1 calculation" + " for modality: " + m);
                logLikelihood[m] = 0;
                continue;
            } else if (Double.isInfinite(logLikelihood[m])) {
                logger.warn("infinite log likelihood at level1 " + " for modality: " + m);
                logLikelihood[m] = 0;
                continue;
            }

            // And the topics
            // Count the number of type-topic pairs that are not just (logGamma(beta[0]) - logGamma(beta[0]))
            int nonZeroTypeTopics = 0;

            for (int type = 0; type < numTypes[m]; type++) {
                // reuse this array as a pointer

                //topicCounts = typeTopicCounts[m][type];
                int index = 0;
                while (index < typeTopicCounts[m][type].length) {

                    int count = typeTopicCounts[m][type][index];
                    if (count > 0) {
                        nonZeroTypeTopics++;
                        //logLikelihood[m] += Dirichlet.logGammaStirling(beta[m] + count);
                        logLikelihood[m] += (beta[m] + count) == 0 ? 0 : Dirichlet.logGammaStirling(beta[m] + count);

                        if (Double.isNaN(logLikelihood[m])) {
                            logger.warn("NaN in log likelihood calculation");
                            logLikelihood[m] = 0;
                            break;
                        } else if (Double.isInfinite(logLikelihood[m])) {
                            logger.warn("infinite log likelihood");
                            logLikelihood[m] = 0;
                            break;
                        }
                    }

                    index++;
                }
            }

            for (int topic = 0; topic < numTopics; topic++) {
                logLikelihood[m] -= (beta[m] * numTypes[m] + tokensPerTopic[m][topic]) == 0 ? 0 : Dirichlet.logGammaStirling((beta[m] * numTypes[m])
                        + tokensPerTopic[m][topic]);
//
//                logLikelihood[m]
//                        -= Dirichlet.logGammaStirling((beta[m] * numTypes[m])
//                                + tokensPerTopic[m][topic]);

                if (Double.isNaN(logLikelihood[m])) {
                    logger.info("NaN after topic " + topic + " " + tokensPerTopic[m][topic]);
                    logLikelihood[m] = 0;
                    continue;
                } else if (Double.isInfinite(logLikelihood[m])) {
                    logger.info("Infinite value after topic " + topic + " " + tokensPerTopic[m][topic]);
                    logLikelihood[m] = 0;
                    continue;
                }

            }

            // logGamma(|V|*beta) for every topic
            logLikelihood[m] += (beta[m] * numTypes[m]) == 0 ? 0 : Dirichlet.logGammaStirling(beta[m] * numTypes[m]) * numTopics;

            // logGamma(beta) for all type/topic pairs with non-zero count
            logLikelihood[m] -= beta[m] == 0 ? 0 : Dirichlet.logGammaStirling(beta[m]) * nonZeroTypeTopics;

            if (Double.isNaN(logLikelihood[m])) {
                logger.info("at the end");
            } else if (Double.isInfinite(logLikelihood[m])) {
                logger.info("Infinite value beta [" + m + "]: " + beta[m] + " * " + numTypes[m]);
                logLikelihood[m] = 0;
            }

        }
        return logLikelihood;
    }

//    /**
//     * Return a tool for estimating topic distributions for new documents
//     */
//    public TopicInferencer getInferencer() {
//        return new TopicInferencer(typeTopicCounts, tokensPerTopic,
//                data.get(0).instance.getDataAlphabet(),
//                alpha, beta[0], betaSum[0]);
//    }
//
//    /**
//     * Return a tool for evaluating the marginal probability of new documents
//     * under this model
//     */
    public MarginalProbEstimator getProbEstimator() {
        return new MarginalProbEstimator(numTopics, alpha[0], (double) gamma[0] * alphaSum[0], beta[0],
                typeTopicCounts[0], tokensPerTopic[0]);
    }
    // Serialization
    private static final long serialVersionUID = 1;
    private static final int CURRENT_SERIAL_VERSION = 0;
    private static final int NULL_INTEGER = -1;

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(CURRENT_SERIAL_VERSION);

        //out.writeObject(data);
        out.writeObject(alphabet);
        //out.writeObject(topicAlphabet);

        out.writeInt(numTopics);

        out.writeObject(numTypes);

        out.writeObject(alpha);
        out.writeObject(alphaSum);
        out.writeObject(beta);
        out.writeObject(betaSum);
        out.writeObject(gamma);

        out.writeObject(typeTopicCounts);
        out.writeObject(tokensPerTopic);

        //out.writeObject(docLengthCounts);
        //out.writeObject(topicDocCounts);
        //out.writeInt(numIterations);
        //out.writeInt(burninPeriod);
        //out.writeInt(saveSampleInterval);
        //out.writeInt(optimizeInterval);
        //out.writeInt(showTopicsInterval);
        //out.writeInt(wordsPerTopic);
        //out.writeInt(saveStateInterval);
        //out.writeObject(stateFilename);
        //out.writeInt(saveModelInterval);
        //out.writeObject(modelFilename);
        //out.writeInt(randomSeed);
        //out.writeObject(formatter);
        //out.writeBoolean(printLogLikelihood);
        //out.writeInt(numThreads);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

        int version = in.readInt();

        //data = (ArrayList<MixTopicModelTopicAssignment>) in.readObject();
        alphabet = (Alphabet[]) in.readObject();
        //topicAlphabet = (LabelAlphabet) in.readObject();

        numTopics = in.readInt();

        numTypes = (int[]) in.readObject();

        alpha = (double[][]) in.readObject();
        alphaSum = (double[]) in.readObject();
        beta = (double[]) in.readObject();
        betaSum = (double[]) in.readObject();
        gamma = (double[]) in.readObject();

        typeTopicCounts = (int[][][]) in.readObject();
        tokensPerTopic = (int[][]) in.readObject();

        //docLengthCounts = (int[][]) in.readObject();
        // topicDocCounts = (int[][][]) in.readObject();
//        numIterations = in.readInt();
//        burninPeriod = in.readInt();
//        saveSampleInterval = in.readInt();
//        optimizeInterval = in.readInt();
//        showTopicsInterval = in.readInt();
//        wordsPerTopic = in.readInt();
//
//        saveStateInterval = in.readInt();
//        stateFilename = (String) in.readObject();
//
//        saveModelInterval = in.readInt();
//        modelFilename = (String) in.readObject();
//
//        randomSeed = in.readInt();
//        formatter = (NumberFormat) in.readObject();
//        printLogLikelihood = in.readBoolean();
//
//        numThreads = in.readInt();
    }

    public void write(File serializedModelFile) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serializedModelFile));
            oos.writeObject(this);
            oos.close();
        } catch (IOException e) {
            System.err.println("Problem serializing ParallelTopicModel to file "
                    + serializedModelFile + ": " + e);
        }
    }

    public static FastQMVWVParallelTopicModel read(File f) throws Exception {

        FastQMVWVParallelTopicModel topicModel = null;

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
        topicModel = (FastQMVWVParallelTopicModel) ois.readObject();
        ois.close();

        //topicModel.initializeHistograms();
        return topicModel;
    }

    public static void main(String[] args) {

        try {

            InstanceList[] training = new InstanceList[1];
            training[0] = InstanceList.load(new File(args[0]));

            int numTopics = args.length > 1 ? Integer.parseInt(args[1]) : 200;

            byte mod = 1;
            FastQMVWVParallelTopicModel lda = new FastQMVWVParallelTopicModel(numTopics, mod, 0.1, 0.01, true, "", true, 0.6, true);
            lda.printLogLikelihood = true;
            lda.setTopicDisplay(50, 7);
            lda.addInstances(training, "", 1, "");

            lda.setNumThreads(Integer.parseInt(args[2]));
            lda.estimate();
            logger.info("printing state");
            lda.printState(new File("state.gz"));
            logger.info("finished printing");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
