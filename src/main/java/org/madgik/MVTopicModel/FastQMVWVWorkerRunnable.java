package org.madgik.MVTopicModel;

import org.madgik.utils.FastQDelta;
import org.madgik.utils.MixTopicModelTopicAssignment;
import org.madgik.utils.FTree;
import java.util.Arrays;
import java.util.ArrayList;

import cc.mallet.types.*;
import org.apache.log4j.Logger;
import cc.mallet.util.Randoms;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Parallel multi-view topic model runnable task using FTrees
 *
 * @author Omiros Metaxas
 */
public class FastQMVWVWorkerRunnable implements Runnable {

    //boolean isFinished = true;
    public static Logger logger = Logger.getLogger("SciTopic");
    public static AtomicInteger newMassCnt = new AtomicInteger(1);
    public static AtomicInteger topicDocMassCnt = new AtomicInteger(1);
    public static AtomicInteger wordFTreeMassCnt = new AtomicInteger(1);
    protected ArrayList<MixTopicModelTopicAssignment> data;
    int startDoc, numDocs;
    protected int numTopics; // Number of topics to be fit
    // These values are used to encode type/topic counts as
    //  count/topic pairs in a single int.
    protected int topicMask;
    protected int topicBits;
    //protected int numTypes;
    public byte numModalities; // Number of modalities
    protected double[][] alpha;	 // low level DP<=>dirichlet(a1,a2,...a is the distribution over topics [epoch][modality][topic]
    protected double[] alphaSum;
    protected double[] beta;   // Prior on per-topic multinomial distribution over words
    protected double[] betaSum;
    protected double[] gamma;
    public static final double DEFAULT_BETA = 0.01;
    protected double[] docSmoothingOnlyMass;
    protected double[][] docSmoothingOnlyCumValues;

    protected double[][] p_a; // a for beta prior for modalities correlation
    protected double[][] p_b; // b for beta prir for modalities correlation

    protected int[][][] typeTopicCounts; // indexed by  [modality][tokentype][topic]
    protected AtomicIntegerArray[] tokensPerTopic; //int[][] tokensPerTopic; // indexed by <topic index>

    //protected int[][][] typeTopicSimilarity; //<modality, token, topic>;
    // for dirichlet estimation
    //protected int[] docLengthCounts; // histogram of document sizes
    //protected int[][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>
    boolean shouldSaveState = false;
    protected FTree[][] trees; //store 
    protected Randoms random;
    protected int threadId = -1;
    protected int nst = -1;
    protected int nut = -1;
    protected List<BlockingQueue<FastQDelta>> queues;
    private final CyclicBarrier cyclicBarrier;
    protected int MHsteps = 1;
    //protected boolean useCycleProposals = false;
    protected ConcurrentSkipListSet<Integer> inActiveTopicIndex;

    protected double[][] expDotProductValues;  //<topic,token>
    protected double[] sumExpValues; // Partition function values per topic     
    protected double useVectorsLambda = 0; //the weight of the p(w|t) probability based on word / topic emdeddings (it should be 0 either when we don't use vectors or we haven;t  calculated related softmax based probabilities (i.e., during burn in period)

    public FastQMVWVWorkerRunnable(
            int numTopics,
            byte numModalities,
            double[][] alpha,
            double[] alphaSum,
            double[] beta,
            double[] betaSum,
            double[] gamma,
            double[] docSmoothingOnlyMass,
            double[][] docSmoothingOnlyCumValues,
            Randoms random,
            ArrayList<MixTopicModelTopicAssignment> data,
            int[][][] typeTopicCounts,
            AtomicIntegerArray[] tokensPerTopic,
            int startDoc,
            int numDocs,
            FTree[][] trees,
            //boolean useCycleProposals,
            int threadId,
            double[][] p_a, // a for beta prior for modalities correlation
            double[][] p_b, // b for beta prir for modalities correlation
            //            ConcurrentLinkedQueue<FastQDelta> queue, 
            CyclicBarrier cyclicBarrier,
            ConcurrentSkipListSet<Integer> inActiveTopicIndex,
            double[][] expDotProductValues, //<topic,token>
            double[] sumExpValues, // Partition function values per topic     
            int nst, //numf of sampling threads
            int nut, //num of updater threads 
            List<BlockingQueue<FastQDelta>> queues
    //, FTree betaSmoothingTree
    ) {

        this.data = data;

        this.threadId = threadId;
        this.nst = nst;
        this.nut = nut;
        //this.queue = queue;
        this.cyclicBarrier = cyclicBarrier;

        this.numTopics = numTopics;
        this.numModalities = numModalities;
        //this.numTypes = typeTopicCounts.length;

        this.typeTopicCounts = typeTopicCounts;
        this.tokensPerTopic = tokensPerTopic;
        this.trees = trees;

        this.alphaSum = alphaSum;
        this.alpha = alpha;
        this.beta = beta;
        this.betaSum = betaSum;
        this.random = random;
        this.gamma = gamma;

        this.startDoc = startDoc;
        this.numDocs = numDocs;
        //this.useCycleProposals = useCycleProposals;
        //this.typeTopicSimilarity = typeTopicSimilarity;

        this.docSmoothingOnlyCumValues = docSmoothingOnlyCumValues;
        this.docSmoothingOnlyMass = docSmoothingOnlyMass;
        this.p_a = p_a;
        this.p_b = p_b;
        this.inActiveTopicIndex = inActiveTopicIndex;
        this.expDotProductValues = expDotProductValues;  //<topic,token>
        this.sumExpValues = sumExpValues; // Partition function values per topic     
        this.queues = queues;
        //System.err.println("WorkerRunnable Thread: " + numTopics + " topics, " + topicBits + " topic bits, " + 
        //				   Integer.toBinaryString(topicMask) + " topic mask");
    }

    /*public void setQueue(BlockingQueue<FastQDelta> queue) {
        this.queue = queue;
    }*/
    public void setUseVectorsLambda(double useVectorsLambda) {
        this.useVectorsLambda = useVectorsLambda;
    }

//    public int[][] getTokensPerTopic() {
//        return tokensPerTopic;
//    }
//
//    public int[][][] getTypeTopicCounts() {
//        return typeTopicCounts;
//    }
//    public int[] getDocLengthCounts() {
//        return docLengthCounts;
//    }
//    public int[][] getTopicDocCounts() {
//        return topicDocCounts;
//    }
//    public void initializeAlphaStatistics(int size) {
////        docLengthCounts = new int[size];
//     //   topicDocCounts = new int[numTopics][size];
//    }
//    public void collectAlphaStatistics() {
//        shouldSaveState = true;
//    }
//    public void resetBeta(double[] beta, double[] betaSum) {
//        this.beta = beta;
//        this.betaSum = betaSum;
//    }
    // p(w|t=z, all) = (alpha[topic] + topicPerDocCounts[d])       *   ( (typeTopicCounts[w][t]/(tokensPerTopic[topic] + betaSum)) + beta/(tokensPerTopic[topic] + betaSum)  )
    // masses:         alphasum     + select a random topics from doc       FTree for active only topics (leave 2-3 spare)                     common FTree f
    //              (binary search)                                               get index from typeTopicsCount
    public void run() {

        try {

            //logger.info("Worker[" + threadId + "] thread started");
            // Initialize the doc smoothing-only sampling bucket (Sum(a[i])
            for (int doc = startDoc;
                    doc < data.size() && doc < startDoc + numDocs;
                    doc++) {

                //if (doc % 50000 == 0) {
                //  logger.info("Worker["+ threadId+"] processing doc " + doc);
                //System.out.println("processing doc " + doc);
                //}
//				
//                FeatureSequence tokenSequence
//                        = (FeatureSequence) data.get(doc).instance.getData();
//                LabelSequence topicSequence
//                        = (LabelSequence) data.get(doc).topicSequence;
//                if (useCycleProposals) {
//                    sampleTopicsForOneDocCyclingProposals(doc);
//                } else {
                sampleTopicsForOneDoc(doc);
                //}

            }

            shouldSaveState = false;
            //isFinished = true;
            logger.info("Worker[" + threadId + "] thread finished");
            for (int utId = 0; utId < nut; utId++) {
                queues.get(nst * utId + threadId).put(new FastQDelta(-1, -1, -1, -1, -1, -1));
            }

            try {
                cyclicBarrier.await();
            } catch (InterruptedException e) {
                System.out.println("Main Thread interrupted!");
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                System.out.println("Main Thread interrupted!");
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int lower_bound(int[] arr, double key, int len) {
        //int len = arr.length;
        int lo = 0;
        int hi = len - 1;
        int mid = (lo + hi) / 2;
        while (true) {
            //int cmp = arr[mid].compareTo(key);
            if (arr[mid] >= key) {
                hi = mid - 1;
                if (hi < lo) {
                    return mid;
                }
            } else {
                lo = mid + 1;
                if (hi < lo) {
                    return mid < len - 1 ? mid + 1 : -1;
                }
            }
            mid = (lo + hi) / 2; //(hi-lo)/2+lo in order not to overflow?  or  (lo + hi) >>> 1
        }
    }

    public static int lower_bound(double[] arr, double key, int len) {
        //int len = arr.length;
        int lo = 0;
        int hi = len - 1;
        int mid = (lo + hi) / 2;
        while (true) {
            //int cmp = arr[mid].compareTo(key);
            if (arr[mid] >= key) {
                hi = mid - 1;
                if (hi < lo) {
                    return mid;
                }
            } else {
                lo = mid + 1;
                if (hi < lo) {
                    return mid < len - 1 ? mid + 1 : -1;
                }
            }
            mid = (lo + hi) / 2; //(hi-lo)/2+lo in order not to overflow?  or  (lo + hi) >>> 1
        }
    }

    public static int lower_bound(Comparable[] arr, Comparable key) {
        int len = arr.length;
        int lo = 0;
        int hi = len - 1;
        int mid = (lo + hi) / 2;
        while (true) {
            int cmp = arr[mid].compareTo(key);
            if (cmp == 0 || cmp > 0) {
                hi = mid - 1;
                if (hi < lo) {
                    return mid;
                }
            } else {
                lo = mid + 1;
                if (hi < lo) {
                    return mid < len - 1 ? mid + 1 : -1;
                }
            }
            mid = (lo + hi) / 2; //(hi-lo)/2+lo in order not to overflow?  or  (lo + hi) >>> 1
        }
    }

    protected void sampleTopicsForOneDoc(int docCnt) {

        try {

            MixTopicModelTopicAssignment doc = data.get(docCnt);

            //double[][] totalMassPerModalityAndTopic = new double[numModalities][];
            //cachedCoefficients = new double[numModalities][numTopics];// Conservative allocation... [nonZeroTopics + 10]; //we want to avoid dynamic memory allocation , thus we think that we will not have more than ten new  topics in each run
            int[][] oneDocTopics = new int[numModalities][]; //token topics sequence for document
            FeatureSequence[] tokenSequence = new FeatureSequence[numModalities]; //tokens sequence

            int[] currentTypeTopicCounts;
            int[] localTopicIndex = new int[numTopics];
            double[] topicDocWordMasses = new double[numTopics];
            int type, oldTopic, newTopic;
            FTree currentTree;

            //int totalLength = 0;
            int[] docLength = new int[numModalities];
            int[][] localTopicCounts = new int[numModalities][numTopics];

            double[] totalMassOtherModalities = new double[numTopics];
            double newTopicMassAllModalities = 0;

            double[][] p = new double[numModalities][numModalities];

            for (byte m = 0; m < numModalities; m++) {

                for (byte j = m; j < numModalities; j++) {

                    double pRand
                            = m == j ? 1.0 : p_a[m][j] == 0 ? 0
                                            : ((double) Math.round(1000 * random.nextBeta(p_a[m][j], p_b[m][j])) / (double) 1000);

                    p[m][j] = (j != 0 && beta[j] == 0.0001) ? 0 : pRand; //too sparse modality --> ignore its doc /topic distribution
                    p[j][m] = (m != 0 && beta[m] == 0.0001) ? 0 : pRand;  //too sparse modality --> ignore its doc /topic distribution
                }

                docLength[m] = 0;

                if (doc.Assignments[m] != null) {
                    //TODO can I order by tokens/topics??
                    oneDocTopics[m] = doc.Assignments[m].topicSequence.getFeatures();

                    //System.arraycopy(oneDocTopics[m], 0, doc.Assignments[m].topicSequence.getFeatures(), 0, doc.Assignments[m].topicSequence.getFeatures().length-1);
                    tokenSequence[m] = ((FeatureSequence) doc.Assignments[m].instance.getData());

                    docLength[m] = tokenSequence[m].getLength(); //size is the same??
                    //totalLength += docLength[m];

                    //		populate topic counts
                    for (int position = 0; position < docLength[m]; position++) {
                        if (oneDocTopics[m][position] == FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {
                            // System.err.println(" Init Sampling UNASSIGNED_TOPIC");
                            continue;
                        }
                        localTopicCounts[m][oneDocTopics[m][position]]++; //, localTopicCounts[m][oneDocTopics[m][position]] + 1);

                    }
                }
            }

            //Share the same distribution proportianl to the length of each modality
//            for (byte m = 0; m < numModalities; m++) {
//
//                for (byte j = 0; j < numModalities; j++) {
//
//                   
//
//                    p[m][j] = (double) docLength[j] / (double)totalLength; //too sparse modality --> ignore its doc /topic distribution
//                    p[j][m] = (double)docLength[j] / (double)totalLength;  //too sparse modality --> ignore its doc /topic distribution
//                }
//            }
            // Build an array that densely lists the topics that
            //  have non-zero counts.
            int denseIndex = 0;
            for (int topic = 0; topic < numTopics; topic++) {
                int i = 0;
                boolean topicFound = false;
                while (i < numModalities && !topicFound) {
                    if (localTopicCounts[i][topic] != 0) {
                        localTopicIndex[denseIndex] = topic;
                        denseIndex++;
                        topicFound = true;
                    }
                    i++;
                }
            }

            // Record the total number of non-zero topics
            int nonZeroTopics = denseIndex;

            for (byte m = 0; m < numModalities; m++) // byte m = 0;
            {
                Arrays.fill(totalMassOtherModalities, 0);

                //calc other modalities mass
                // if (m != 0) { //main (reference) modality 
                for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {

                    int topic = localTopicIndex[denseIndex];
                    for (byte i = 0; i < numModalities; i++) {
                        if (i != m && docLength[i] != 0) {
                            totalMassOtherModalities[topic] += p[m][i] * (localTopicCounts[i][topic] + gamma[i] * alpha[i][topic]) / (docLength[i] + (double) gamma[i] * alphaSum[i]);

                        }
                    }

                    totalMassOtherModalities[topic] = totalMassOtherModalities[topic] * (docLength[m] + (double) gamma[m] * alphaSum[m]);
                }

//new topic Mass 
                newTopicMassAllModalities = 0;
                for (byte i = 0; i < numModalities; i++) {
                    newTopicMassAllModalities += p[m][i] * (gamma[i] * alpha[i][numTopics]) / (docLength[i] + (double) gamma[i] * alphaSum[i]);
                    //*currentTypeTopicCounts.length);
                }
                newTopicMassAllModalities = newTopicMassAllModalities * (docLength[m] + (double) gamma[m] * alphaSum[m]);
                //newTopicMass
                // }

                FeatureSequence tokenSequenceCurMod = tokenSequence[m];

                //	Iterate over the positions (words) in the document 
                for (int position = 0; position < docLength[m]; position++) {
                    type = tokenSequenceCurMod.getIndexAtPosition(position);
                    oldTopic = oneDocTopics[m][position];

                    currentTypeTopicCounts = typeTopicCounts[m][type];
                    currentTree = trees[m][type];

                    if (oldTopic != FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {

                        // Decrement the local doc/topic counts
                        localTopicCounts[m][oldTopic]--;

                        // Maintain the dense index, if we are deleting
                        //  the old topic
                        boolean isDeletedTopic = localTopicCounts[m][oldTopic] == 0;
                        byte jj = 0;
                        while (isDeletedTopic && jj < numModalities) {
                            // if (jj != m) { //do not check m twice
                            isDeletedTopic = localTopicCounts[jj][oldTopic] == 0;
                            // }
                            jj++;
                        }

                        //isDeletedTopic = false;//todo omiros test
                        if (isDeletedTopic) {

                            // First get to the dense location associated with  the old topic.
                            denseIndex = 0;
                            // We know it's in there somewhere, so we don't  need bounds checking.
                            while (localTopicIndex[denseIndex] != oldTopic) {
                                denseIndex++;
                            }
                            // shift all remaining dense indices to the left.
                            while (denseIndex < nonZeroTopics) {
                                if (denseIndex < localTopicIndex.length - 1) {
                                    localTopicIndex[denseIndex]
                                            = localTopicIndex[denseIndex + 1];
                                }
                                denseIndex++;
                            }
                            nonZeroTopics--;
                        }

                        // Decrement the global type topic counts  at the end (through delta / queue)
                    }
                    //else {
//                        int test = 1;
//                    }

                    //If WordVect
                    newTopic = -1;
                    /*  if (useTypeVectors) {
                        double nextUniform = ThreadLocalRandom.current().nextDouble();
                        if (nextUniform > useTypeVectorsProb) { //TODO: Use MH instead (or additionaly)
                            double sample = ThreadLocalRandom.current().nextDouble() * typeTopicSimilarity[type][oldTopic][numTopics-1];

                            newTopic = lower_bound(typeTopicSimilarity[type][oldTopic], sample, numTopics);

                            if (newTopic == -1) {
                                System.err.println("WorkerRunnable sampling error on word topic mass: " + sample + " " + trees[m][type].tree[1]);
                                //newTopic = numTopics - 1; // sample using topicMasses regular process 
                                //throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
                            }
                        }
                    }*/
                    //public boolean trainTypeVectors;

                    //		compute word / doc mass for binary search
                    if (newTopic == -1) {
                        double topicDocWordMass = 0.0;

                        //TODO: 1) based on weight select to sample either based on counts or typeTopicSimilarity --> select p(w|t)
                        //      2) p(w|t) based on vectors --> either based on softmax or cosine similarity 
                        //   we need a mass per typeTopicSimilarity[type][oldTopic] and binary search on it (?) (double) typeTopicSimilarity[type][oldTopic][topic] / 10000 * 
                        for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
                            int topic = localTopicIndex[denseIndex];
                            int n = localTopicCounts[m][topic];
                            double p_wt = (useVectorsLambda != 0 && m == 0)
                                    ? (useVectorsLambda * (expDotProductValues[topic][type] / sumExpValues[topic]) + (1 - useVectorsLambda)
                                    * ((currentTypeTopicCounts[topic] + beta[m]) / (tokensPerTopic[m].get(topic) + betaSum[m])))
                                    : (currentTypeTopicCounts[topic] + beta[m]) / (tokensPerTopic[m].get(topic) + betaSum[m]);

                            topicDocWordMass += (p[m][m] * n + totalMassOtherModalities[topic]) * p_wt; //(currentTypeTopicCounts[topic] + beta[m]) / (tokensPerTopic[m][topic] + betaSum[m]);
                            //topicDocWordMass +=  n * trees[type].getComponent(topic);
                            topicDocWordMasses[denseIndex] = topicDocWordMass;

                        }

                        double newTopicMass = inActiveTopicIndex.isEmpty() ? 0 : newTopicMassAllModalities / currentTypeTopicCounts.length;//check this

                        double nextUniform = ThreadLocalRandom.current().nextDouble();
                        //samplingWeights[(int) Math.round(nextUniform * 100)] += 1;
                        double sample = nextUniform * (newTopicMass + topicDocWordMass + currentTree.tree[1]);

                        //double sample = ThreadLocalRandom.current().nextDouble() * (topicDocWordMass + trees[type].tree[1]);
                        if (sample < newTopicMass) {
                            newMassCnt.getAndIncrement();

                            newTopic = inActiveTopicIndex.first();//ThreadLocalRandom.current().nextInt(inActiveTopicIndex.size()));
                            System.out.println("Sample new topic: " + newTopic);
                        } else {
                            sample -= newTopicMass;
                            if (sample < topicDocWordMass) {
                                topicDocMassCnt.getAndIncrement();
                                newTopic = localTopicIndex[lower_bound(topicDocWordMasses, sample, nonZeroTopics)];
                            } else {
                                wordFTreeMassCnt.getAndIncrement();
                                double nextUniform2 = ThreadLocalRandom.current().nextDouble(); //if we use nextUniform we are biased towards large numbers as small ones will lead to newTopicMass + topicDocWordMass
                                newTopic = currentTree.sample(nextUniform2);
                            }

                        }
//            if (sample < topicDocWordMass) {
//
//                //int tmp = lower_bound(topicDocWordMasses, sample, nonZeroTopics);
//                newTopic = localTopicIndex[lower_bound(topicDocWordMasses, sample, nonZeroTopics)]; //actual topic
//
//            } else {
//
//                newTopic = currentTree.sample(nextUniform);
//            }

                        if (newTopic == -1) {
                            System.err.println("WorkerRunnable sampling error on word topic mass: " + sample + " " + trees[m][type].tree[1]);
                            newTopic = numTopics - 1; // TODO is this appropriate
                            //throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
                        }
                    }
                    //assert(newTopic != -1);
                    //			Put that new topic into the counts
                    oneDocTopics[m][position] = newTopic;

                    //increment local counts
                    localTopicCounts[m][newTopic]++;

                    // If this is a new topic for this document, add the topic to the dense index.
                    boolean isNewTopic = (localTopicCounts[m][newTopic] == 0);
                    byte jj = 0;
                    while (isNewTopic && jj < numModalities) {
                        //if (jj != m) { // every other topic should have zero counts
                        isNewTopic = localTopicCounts[jj][newTopic] == 0;
                        //}
                        jj++;
                    }

                    if (isNewTopic) {
                        // First find the point where we  should insert the new topic by going to
                        //  the end  and working backwards
                        denseIndex = nonZeroTopics;
                        while (denseIndex > 0
                                && localTopicIndex[denseIndex - 1] > newTopic) {
                            localTopicIndex[denseIndex]
                                    = localTopicIndex[denseIndex - 1];
                            denseIndex--;
                        }
                        localTopicIndex[denseIndex] = newTopic;
                        nonZeroTopics++;
                    }

                    //add delta to the queue
                    if (newTopic != oldTopic) {
                        //queue.add(new FastQDelta(oldTopic, newTopic, type, 0, 1, 1));
                        queues.get(nst * (type % nut) + threadId).put(new FastQDelta(oldTopic, newTopic, type, m, oldTopic == FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC ? 0 : localTopicCounts[m][oldTopic], localTopicCounts[m][newTopic]));
//                        if (queue.size()>200)
//                        {                          
//                            System.out.println("Thread["+threadId+"] queue size="+queue.size());
//                        }
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

//        if (shouldSaveState) {
//            // Update the document-topic count histogram,
//            //  for dirichlet estimation
//            //[docLength]++;
//
//            for (int topic = 0; topic < numTopics; topic++) {
//                topicDocCounts[topic][localTopicCounts[topic]]++;
//            }
//        }
    }

//    protected void sampleTopicsForOneDocCyclingProposals(int docCnt) {
//
//        MixTopicModelTopicAssignment doc = data.get(docCnt);
//
//        //double[][] totalMassPerModalityAndTopic = new double[numModalities][];
//        //cachedCoefficients = new double[numModalities][numTopics];// Conservative allocation... [nonZeroTopics + 10]; //we want to avoid dynamic memory allocation , thus we think that we will not have more than ten new  topics in each run
//        int[][] oneDocTopics = new int[numModalities][]; //token topics sequence for document
//        FeatureSequence[] tokenSequence = new FeatureSequence[numModalities]; //tokens sequence
//
//        int[] currentTypeTopicCounts;
//        //int[] localTopicIndex = new int[numTopics];
//        //double[] topicDocWordMasses = new double[numTopics];
//        int type, oldTopic, newTopic;
//        FTree currentTree;
//
//        int[] docLength = new int[numModalities];
//        int[][] localTopicCounts = new int[numModalities][numTopics];
//
//        double[] totalMassOtherModalities = new double[numTopics];
//
//        double[] totalMassOtherModalitiesCumValues = new double[numTopics];
//        double totalNormMassOther = 0;
//
//        double[][] p = new double[numModalities][numModalities];
//
//        for (byte m = 0; m < numModalities; m++) {
//
//            for (byte j = m; j < numModalities; j++) {
//                double pRand = m == j ? 1.0 : p_a[m][j] == 0 ? 0 : ((double) Math.round(1000 * random.nextBeta(p_a[m][j], p_b[m][j])) / (double) 1000);
//
//                p[m][j] = pRand;
//                p[j][m] = pRand;
//            }
//
//            docLength[m] = 0;
//
//            totalNormMassOther = 0;
//
//            if (doc.Assignments[m] != null) {
//                //TODO can I order by tokens/topics??
//                oneDocTopics[m] = doc.Assignments[m].topicSequence.getFeatures();
//
//                //System.arraycopy(oneDocTopics[m], 0, doc.Assignments[m].topicSequence.getFeatures(), 0, doc.Assignments[m].topicSequence.getFeatures().length-1);
//                tokenSequence[m] = ((FeatureSequence) doc.Assignments[m].instance.getData());
//
//                docLength[m] = tokenSequence[m].getLength(); //size is the same??
//
//                //		populate topic counts
//                for (int position = 0; position < docLength[m]; position++) {
//                    if (oneDocTopics[m][position] == FastQMVParallelTopicModel.UNASSIGNED_TOPIC) {
//                        System.err.println(" Init Sampling UNASSIGNED_TOPIC");
//                        continue;
//                    }
//                    localTopicCounts[m][oneDocTopics[m][position]]++; //, localTopicCounts[m][oneDocTopics[m][position]] + 1);
//
//                }
//            }
//        }
//
//        for (byte m = 0; m < numModalities; m++) // byte m = 0;
//        {
//            Arrays.fill(totalMassOtherModalities, 0);
//            Arrays.fill(totalMassOtherModalitiesCumValues, 0);
//            totalNormMassOther = 0;
//
//            //calc other modalities mass
//            for (int topic = 0; topic < numTopics; topic++) {
//
//                for (byte i = 0; i < numModalities; i++) {
//                    if (i != m && docLength[i] != 0) {
//                        totalMassOtherModalities[topic] += p[m][i] * localTopicCounts[i][topic] / docLength[i];
//                    }
//                }
//
//                totalNormMassOther += totalMassOtherModalities[topic];
//                totalMassOtherModalitiesCumValues[topic] = totalNormMassOther;
//            }
//
//            totalNormMassOther = totalNormMassOther * (docLength[m] + alphaSum[m]);
//
//            FeatureSequence tokenSequenceCurMod = tokenSequence[m];
//
//            //	Iterate over the positions (words) in the document 
//            for (int position = 0; position < docLength[m]; position++) {
//                type = tokenSequenceCurMod.getIndexAtPosition(position);
//                oldTopic = oneDocTopics[m][position];
//
//                currentTypeTopicCounts = typeTopicCounts[m][type];
//                currentTree = trees[m][type];
//
//                if (oldTopic != FastQMVParallelTopicModel.UNASSIGNED_TOPIC) {
//
//                    // Decrement the local doc/topic counts
//                    localTopicCounts[m][oldTopic]--;
//
//                    // Multi core (queue based) approximation: All global counts will be updated at the end 
//                }
//
//                int currentTopic = oldTopic;
//
//                for (int MHstep = 0; MHstep < MHsteps; MHstep++) {
//                //in every Metropolis hasting step we are taking two samples cycling related doc & word proposals and we eventually keep the best one
//                    // we can sample from doc topic mass immediately after word topic mass, as it doesn't get affected by current topic selection!! 
//                    //--> thus we get & check both samples from cycle proposal in every step
//
//                    //  if (!useDocProposal) {
//                    //Sample Word topic mass
//                    double sample = ThreadLocalRandom.current().nextDouble();//* (trees[type].tree[1]);
//
//                    //	Make sure it actually gets set
//                    newTopic = -1;
//
//                    newTopic = currentTree.sample(sample);
//
//                    if (newTopic == -1) {
//                        System.err.println("WorkerRunnable sampling error on word topic mass: " + sample + " " + trees[m][type].tree[1]);
//                        newTopic = numTopics - 1; // TODO is this appropriate
//                        //throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
//                    }
//
//                    if (currentTopic != newTopic) {
//                        // due to queue based multi core approximation we should decrement global counts whenever appropriate
//                        //both decr/incr of global arrays and trees is happening at the end... So at this point they contain the current (not decreased values)
//                        // model_old & model_new should be based on decreased values, whereas probabilities (prop_old & prop_new) on the current ones
//                        // BUT global cnts are changing due to QUeue based updates so there is no meaning in them
//                        double model_old = (localTopicCounts[m][currentTopic] + gamma[m] * alpha[m][currentTopic] + totalMassOtherModalities[currentTopic] * (docLength[m] + alphaSum[m]))
//                                * (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);
//                        double model_new = (localTopicCounts[m][newTopic] + gamma[m] * alpha[m][newTopic] + totalMassOtherModalities[newTopic] * (docLength[m] + alphaSum[m]))
//                                * (currentTypeTopicCounts[newTopic] + beta[m]) / (tokensPerTopic[m][newTopic] + betaSum[m]);
//                        double prop_old = (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);
//                        double prop_new = (currentTypeTopicCounts[newTopic] + beta[m]) / (tokensPerTopic[m][newTopic] + betaSum[m]);
//                        double acceptance = (model_new * prop_old) / (model_old * prop_new);
//
////                    double prop_old2 = (oldTopic == currentTopic)
////                            ? (localTopicCounts[currentTopic] + 1 + alpha[currentTopic])
////                            : (localTopicCounts[currentTopic] + alpha[currentTopic]);
////                    double prop_new2 = (newTopic == oldTopic)
////                            ? (localTopicCounts[newTopic] + 1 + alpha[newTopic])
////                            : (localTopicCounts[newTopic] + alpha[newTopic]);
////                    acceptance = (temp_new * prop_old * prop_new2) / (temp_old * prop_new * prop_old2);
////                    
//                        //3. Compare against uniform[0,1]
//                        currentTopic = acceptance >= 1 ? newTopic : ThreadLocalRandom.current().nextDouble() < acceptance ? newTopic : currentTopic;
////                    if (acceptance >= 1 || random.nextUniform() < acceptance) {
////                        currentTopic = newTopic;
////                    }
//                    }
//
//                    // Sample Doc topic mass 
//                    double newTopicMass = inActiveTopicIndex.isEmpty() ? 0 : gamma[m] * alpha[m][numTopics] / (typeTopicCounts[m].length);
//                    sample = ThreadLocalRandom.current().nextDouble() * (newTopicMass + docSmoothingOnlyMass[m] + totalNormMassOther + docLength[m] - 1);
//                    double origSample = sample;
//
//                    //	Make sure it actually gets set
//                    newTopic = -1;
//
//                    if (sample < newTopicMass) {
//                        newTopic = inActiveTopicIndex.get(0);//ThreadLocalRandom.current().nextInt(inActiveTopicIndex.size()));
//                    } else {
//                        sample -= newTopicMass;
//                        if (sample < docSmoothingOnlyMass[m]) {
//
//                            newTopic = lower_bound(docSmoothingOnlyCumValues[m], sample, numTopics);
//
//                        } else {
//                            sample -= docSmoothingOnlyMass[m];
//
//                            if (sample < totalNormMassOther) //Other modalities mass
//                            {
//
//                                sample = sample / (docLength[m] + alphaSum[m]);
//                                newTopic = lower_bound(totalMassOtherModalitiesCumValues, sample, numTopics);
//                            } else { //just select one random topic from DocTopics excluding the current one
//                                sample -= totalNormMassOther;
//                                int tmpPos = (int) sample < position ? (int) sample : (int) sample + 1;
//                                newTopic = oneDocTopics[m][tmpPos];
//
//                            }
//                        }
//                    }
//
//                    if (newTopic == -1) {
//                        System.err.println("WorkerRunnable sampling error on doc topic mass: " + origSample + " " + sample + " " + docSmoothingOnlyMass);
//                        newTopic = numTopics - 1; // TODO is this appropriate
//                        //throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
//                    }
//
//                    if (currentTopic != newTopic) {
//                        //both decr/incr of global arrays and trees is happening at the end... So at this point they contain the current (not decreased values)
//                        // model_old & model_new should be based on decreased values, whereas probabilities (prop_old & prop_new) on the current ones
//
//                        double model_old = (localTopicCounts[m][currentTopic] + gamma[m] * alpha[m][currentTopic] + totalMassOtherModalities[currentTopic] * (docLength[m] + alphaSum[m]))
//                                * (currentTypeTopicCounts[currentTopic] + beta[m]) / (tokensPerTopic[m][currentTopic] + betaSum[m]);
//                        double model_new = (localTopicCounts[m][newTopic] + gamma[m] * alpha[m][newTopic] + totalMassOtherModalities[newTopic] * (docLength[m] + alphaSum[m]))
//                                * (currentTypeTopicCounts[newTopic] + beta[m]) / (tokensPerTopic[m][newTopic] + betaSum[m]);
//                        double prop_old = (oldTopic == currentTopic)
//                                ? (localTopicCounts[m][currentTopic] + 1 + gamma[m] * alpha[m][currentTopic] + totalMassOtherModalities[currentTopic] * (docLength[m] + alphaSum[m]))
//                                : (localTopicCounts[m][currentTopic] + gamma[m] * alpha[m][currentTopic] + totalMassOtherModalities[currentTopic] * (docLength[m] + alphaSum[m]));
//                        double prop_new = (newTopic == oldTopic)
//                                ? (localTopicCounts[m][newTopic] + 1 + gamma[m] * alpha[m][newTopic] + totalMassOtherModalities[newTopic] * (docLength[m] + alphaSum[m]))
//                                : (localTopicCounts[m][newTopic] + gamma[m] * alpha[m][newTopic] + totalMassOtherModalities[newTopic] * (docLength[m] + alphaSum[m]));
//                        double acceptance = (model_new * prop_old) / (model_old * prop_new);
//                        //acceptance = (temp_new * prop_old * trees[type].getComponent(newTopic)) / (temp_old * prop_new * trees[type].getComponent(oldTopic));
//
////                    double prop_old2 = (currentTypeTopicCounts[currentTopic] + beta) / (tokensPerTopic[currentTopic] + betaSum);
////                    double prop_new2 = (currentTypeTopicCounts[newTopic] + beta) / (tokensPerTopic[newTopic] + betaSum);
////                    acceptance = (model_new * prop_old * prop_new2) / (model_old * prop_new * prop_old2);
////                    
//                        //3. Compare against uniform[0,1]
//                        //currentTopic = acceptance >= 1 ? newTopic : random.nextUniform() < acceptance ? newTopic : currentTopic;
//                        currentTopic = acceptance >= 1 ? newTopic : ThreadLocalRandom.current().nextDouble() < acceptance ? newTopic : currentTopic;
//
////                    if (acceptance >= 1 || random.nextUniform() < acceptance) {
////                        currentTopic = newTopic;
////                    }
//                    }
//
//                    // }
//                }
//
//                //assert(newTopic != -1);
//                //			Put that new topic into the counts
//                oneDocTopics[m][position] = currentTopic;
//
//                localTopicCounts[m][currentTopic]++;
//
//                if (currentTopic != oldTopic) {
//
//                    queue.add(new FastQDelta(oldTopic, currentTopic, type, m, localTopicCounts[m][oldTopic], localTopicCounts[m][currentTopic]));
//
//                }
//
//            }
//
////        if (shouldSaveState) {
////            // Update the document-topic count histogram,
////            //  for dirichlet estimation
////            //[docLength]++;
////
////            for (int topic = 0; topic < numTopics; topic++) {
////                topicDocCounts[topic][localTopicCounts[topic]]++;
////            }
////        }
//        }
//    }
}
