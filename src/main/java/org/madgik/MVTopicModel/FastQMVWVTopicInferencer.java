/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.MVTopicModel;

/**
 *
 * @author ometaxas
 */
import cc.mallet.pipe.Pipe;
import cc.mallet.topics.TopicAssignment;
import cc.mallet.types.*;
import cc.mallet.util.*;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.Arrays;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.log4j.Logger;
//import static org.scitopic.MVTopicModel.FastQMVWVParallelTopicModel.logger;
import org.madgik.utils.FTree;

import org.madgik.utils.MixTopicModelTopicAssignment;

public class FastQMVWVTopicInferencer implements Serializable {

    public static Logger logger = Logger.getLogger("SciTopic");

    protected Pipe[] pipes; //pipes for InstanceList (it will be used on incremental training and inference)
    protected int[] numTypes; // per modality
    protected double[][] alpha;	 // Dirichlet(alpha,alpha,...) is the distribution over topics
    protected double[] alphaSum;
    protected double[] beta;   // Prior on per-topic multinomial distribution over words
    protected double[] betaSum;
    protected double[] gamma;
    protected int numTopics; // Number of topics to be fit

    //we should only have one updating thread that updates global counts, 
    // otherwise use AtomicIntegerArray for tokensPerTopic and split typeTopicCounts in such a way that only one thread updates topicCounts for a specific type
    protected int[][][] typeTopicCounts; //
    protected AtomicIntegerArray[] tokensPerTopic; // indexed by <topic index> 
    protected Randoms random;

    protected byte numModalities; // Number of modalities
    protected double[] docSmoothingOnlyMass;
    protected double[][] docSmoothingOnlyCumValues;
    protected double[][] p_a; // a for beta prior for modalities correlation
    protected double[][] p_b; // b for beta prir for modalities correlation
    protected double[] discrWeightPerModality;
    protected double[][] pMean; // modalities correlation

    //global params (not external)
    protected FTree[][] trees; //store 
    protected Alphabet[] alphabet; // the alphabet for the input data
    protected ArrayList<MixTopicModelTopicAssignment> data;  // the training instances and their topic assignments
    protected LabelAlphabet topicAlphabet;  // the alphabet for the topics
    protected int numIterations = 10;

    public FastQMVWVTopicInferencer(Pipe[] pipes, int[] numTypes, double[][] alpha, double[] alphaSum, int[][][] typeTopicCounts, AtomicIntegerArray[] tokensPerTopic,
            double[] beta, double[] betaSum, double[] gamma, int numTopics, byte numModalities, double[] docSmoothingOnlyMass,
            double[][] docSmoothingOnlyCumValues, // FTree[][] trees, 
            double[][] p_a, double[][] p_b, double[] discrWeightPerModality, double[][] pMean) {

        this.pipes = pipes;
        this.numTypes = numTypes;
        this.alpha = alpha;
        this.alphaSum = alphaSum;
        this.typeTopicCounts = typeTopicCounts;
        this.tokensPerTopic = tokensPerTopic;
        this.beta = beta;
        this.betaSum = betaSum;
        this.gamma = gamma;
        this.numTopics = numTopics;
        this.numModalities = numModalities;
        this.docSmoothingOnlyMass = docSmoothingOnlyMass;
        this.docSmoothingOnlyCumValues = docSmoothingOnlyCumValues;
        //this.trees = trees;
        this.p_a = p_a;
        this.p_b = p_b;
        this.discrWeightPerModality = discrWeightPerModality;
        this.pMean = pMean;
        
        initInferencer();
    }

    public FastQMVWVTopicInferencer() {
    }

    public void setRandomSeed(int seed) {
        random = new Randoms(seed);
    }

    public Pipe[] getPipes() {
        return pipes;
    }

    public void inferTopicDistributionsOnNewDocs(InstanceList[] training, String SQLConnectionString, String experimentId, PrintWriter out) throws IOException {

        TObjectIntHashMap<String> entityPosition = new TObjectIntHashMap<String>();

        for (Byte m = 0; m < numModalities; m++) {
            pipes[m] = training[m].getPipe();
            alphabet[m] = training[m].getDataAlphabet();
            //numTypes[m] = alphabet[m].size(); //We need to keep previous (initial) size

            String modInfo = "Modality<" + m + ">[" + (training[m].size() > 0 ? training[m].get(0).getSource().toString() : "-") + "] Size:" + training[m].size() + " Alphabet count: " + numTypes[m];
            logger.info(modInfo);

            betaSum[m] = beta[m] * numTypes[m];
            int doc = 0;

            for (Instance instance : training[m]) {
                doc++;
                long iterationStart = System.currentTimeMillis();

                FeatureSequence tokens = (FeatureSequence) instance.getData();

                LabelSequence topicSequence
                        = new LabelSequence(topicAlphabet, new int[tokens.size()]);

                TopicAssignment t = new TopicAssignment(instance, topicSequence);

                MixTopicModelTopicAssignment mt;
                String entityId = (String) instance.getName();

                int index = -1;

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

        // End of initialization 
        // Init topic assignments 
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

                        //  check if it is out-of-vocabulary term
                        if (type < numTypes[m] ) {
                            FTree currentTree = trees[m][type];
                            double nextUniform2 = ThreadLocalRandom.current().nextDouble();
                            topic = currentTree.sample(nextUniform2);
                        } else {
                            continue;
                        }

                        topics[position] = topic;

                    }
                }
            }
        }

        // End of Init topic assignments 
        // Sampling phase
        long startTime = System.currentTimeMillis();
        int nst = 1; //number of sampling threads
        int nut = 0; // number of model updating threads 
        final CyclicBarrier barrier = new CyclicBarrier(nst + 1);//plus one for the current thread 

        FastQMVWVWorkerRunnable[] samplingRunnables = new FastQMVWVWorkerRunnable[nst];

        int docsPerThread = data.size() / nst;
        int offset = 0;

        //pDistr_Var = new double[numModalities][numModalities][data.size()];
        for (byte i = 0; i < numModalities; i++) {
            Arrays.fill(this.p_a[i], 0.2d);
            Arrays.fill(this.p_b[i], 1d);
        }

        for (int thread = 0; thread < nst; thread++) {

            // some docs may be missing at the end due to integer division
            if (thread == nst - 1) {
                docsPerThread = data.size() - offset;
            }

            ConcurrentSkipListSet<Integer> inActiveTopicIndex = new ConcurrentSkipListSet<Integer>();

            samplingRunnables[thread] = new FastQMVWVWorkerRunnable(
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
                    null, //expDotProductValues,
                    null, //sumExpValues,
                    nst,
                    nut,
                    null //no model updating queues
            );

            offset += docsPerThread;

        }

        ExecutorService executor = Executors.newFixedThreadPool(nst + nut);

        for (int iteration = 1; iteration <= numIterations; iteration++) {

            long iterationStart = System.currentTimeMillis();

            for (int thread = 0; thread < nst; thread++) {

                executor.submit(samplingRunnables[thread]);
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

            long elapsedMillis = System.currentTimeMillis() - iterationStart;
            if (elapsedMillis < 5000) {
                logger.info(elapsedMillis + "ms ");
            } else {
                logger.info((elapsedMillis / 1000) + "s ");
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

        //End of Sampling phase
        //save results
        double threshold = 0.03;
        int max = -1;
        printDocumentTopics(out, threshold, max, SQLConnectionString, experimentId);
    }

    public void printDocumentTopics(PrintWriter out, double threshold, int max, String SQLConnectionString, String experimentId) {
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

        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLConnectionString.isEmpty()) {
                connection = DriverManager.getConnection(SQLConnectionString);
                statement = connection.createStatement();
            }

            PreparedStatement bulkInsert = null;
            String sql = "insert into doc_topic values(?,?,?,?,?);";

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
                    
                    PreparedStatement deletePrevious = connection.prepareStatement(String.format("Delete from doc_topic where  ExperimentId = '%s' and docid='%s' ", experimentId, docId));
                    deletePrevious.executeUpdate();

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
                            //I reweight each modality's contribution in the proportion of the document based on its discrimination power (skew index) and its relation to text
                            topicProportion += (m == 0 ? 1 : discrWeightPerModality[m]) * pMean[0][m] * ((double) topicCounts[m][topic] + (double) gamma[m] * alpha[m][topic]) / (docLen[m] + (double) gamma[m] * alphaSum[m]);
                            normalizeSum += (m == 0 ? 1 : discrWeightPerModality[m]) * pMean[0][m];
                        }
                        sortedTopics[topic].set(topic, (topicProportion / normalizeSum));

                    }

                    Arrays.sort(sortedTopics);

                    for (int i = 0; i < max; i++) {
                        if (sortedTopics[i].getWeight() < threshold) {
                            break;
                        }

                        builder.append(sortedTopics[i].getID() + "\t"
                                + sortedTopics[i].getWeight() + "\t");
                        if (out != null) {
                            out.println(builder);
                        }

                        if (!SQLConnectionString.isEmpty()) {

                            bulkInsert.setString(1, docId);
                            bulkInsert.setInt(2, sortedTopics[i].getID());
                            bulkInsert.setDouble(3, (double) Math.round(sortedTopics[i].getWeight() * 10000) / 10000);
                            bulkInsert.setString(4, experimentId);
                            bulkInsert.setBoolean(5, true);
                            bulkInsert.executeUpdate();

                        }

                    }

//                    if ((doc / 10) * 10 == doc && !sql.isEmpty()) {
//                        statement.executeUpdate(sql);
//                        sql = "";
//                    }
                }
                if (!SQLConnectionString.isEmpty()) {
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
    // Serialization
    private static final long serialVersionUID = 1;
    private static final int CURRENT_SERIAL_VERSION = 0;
    private static final int NULL_INTEGER = -1;

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(CURRENT_SERIAL_VERSION);

        out.writeObject(pipes);
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

        out.writeByte(numModalities);
        out.writeObject(docSmoothingOnlyMass);
        out.writeObject(docSmoothingOnlyCumValues);
        //out.writeObject(trees);
        out.writeObject(p_a);
        out.writeObject(p_b);
        out.writeObject(discrWeightPerModality);
        out.writeObject(pMean);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

        int version = in.readInt();

        pipes = (Pipe[]) in.readObject();
        //topicAlphabet = (LabelAlphabet) in.readObject();

        numTopics = in.readInt();

        numTypes = (int[]) in.readObject();

        alpha = (double[][]) in.readObject();
        alphaSum = (double[]) in.readObject();
        beta = (double[]) in.readObject();
        betaSum = (double[]) in.readObject();
        gamma = (double[]) in.readObject();

        typeTopicCounts = (int[][][]) in.readObject();
        tokensPerTopic = (AtomicIntegerArray[]) in.readObject();
        numModalities = in.readByte();
        docSmoothingOnlyMass = (double[]) in.readObject();
        docSmoothingOnlyCumValues = (double[][]) in.readObject();
        //trees = (FTree[][]) in.readObject();
        p_a = (double[][]) in.readObject();
        p_b = (double[][]) in.readObject();
        discrWeightPerModality = (double[]) in.readObject();
        pMean = (double[][]) in.readObject();

        initInferencer();

    }

    private void initInferencer() {
        this.data = new ArrayList<MixTopicModelTopicAssignment>();
        this.random = new Randoms();
        this.alphabet = new Alphabet[numModalities];
        this.numIterations = 10;
        
        //build trees per type
        trees = new FTree[numModalities][];
        double[] temp = new double[numTopics];

        for (Byte m = 0; m < numModalities; m++) {
            trees[m] = new FTree[numTypes[m]];
            for (int w = 0; w < numTypes[m]; ++w) {

                //reset temp
                Arrays.fill(temp, 0);

                int[] currentTypeTopicCounts = typeTopicCounts[m][w];
                for (int currentTopic = 0; currentTopic < numTopics; currentTopic++) {
                    temp[currentTopic] = (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m].get(currentTopic) + betaSum[m]);

                }

                //trees[w].init(numTopics);
                trees[m][w] = new FTree(temp);

            }

        }
    }

    public static FastQMVWVTopicInferencer read(String SQLConnectionString, String experimentId) {

        FastQMVWVTopicInferencer topicModel = null;
        Connection connection = null;
        Statement statement = null;
        try {
            // create a database connection
            if (!SQLConnectionString.isEmpty()) {
                connection = DriverManager.getConnection(SQLConnectionString);
                statement = connection.createStatement();

                String modelSelect = String.format("select inferencemodel from experiment "
                        + "where ExperimentId = '%s' \n",
                        experimentId);

                ResultSet rs = statement.executeQuery(modelSelect);
                while (rs.next()) {
                    try {
                        byte b[] = (byte[]) rs.getObject("inferencemodel");
                        ByteArrayInputStream bi = new ByteArrayInputStream(b);
                        ObjectInputStream si = new ObjectInputStream(bi);
                        topicModel = (FastQMVWVTopicInferencer) si.readObject();
                    } catch (Exception e) {
                        logger.error(e.getMessage());

                    }

                }

// deserialize the object
            }
        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found

            logger.error(e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e.getMessage());

            }
        }

        return topicModel;
    }

    public static FastQMVWVTopicInferencer read(String fileName) {
        //currentAlphabet.lookupIndex(singular)
        try {
            FastQMVWVTopicInferencer inferencer = FastQMVWVTopicInferencer.read(new File(fileName));
            return inferencer;
        } catch (Exception e) {
            logger.error("File input error:");
            return null;
        }
    }

    public static FastQMVWVTopicInferencer read(File f) throws Exception {

        FastQMVWVTopicInferencer inferencer = null;

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
        inferencer = (FastQMVWVTopicInferencer) ois.readObject();
        ois.close();

        return inferencer;
    }

    public void write(File serializedModelFile) {
        try {

            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serializedModelFile));
            oos.writeObject(this);
            oos.close();
        } catch (IOException e) {
            System.err.println("Problem serializing Inferencer to file "
                    + serializedModelFile + ": " + e);
        }
    }

}
