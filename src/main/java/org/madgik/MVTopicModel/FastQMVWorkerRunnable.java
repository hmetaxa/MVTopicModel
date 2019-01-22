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
import java.util.concurrent.BrokenBarrierException;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parallel multi-view topic model runnable task using FTrees
 *
 * @author Omiros Metaxas
 */
public class FastQMVWorkerRunnable implements Runnable {

    //boolean isFinished = true;
    public static Logger logger = Logger.getLogger(PTMFlow.class.getName());
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
    protected int[][] tokensPerTopic; // indexed by <topic index>

    //protected int[][][] typeTopicSimilarity; //<modality, token, topic>;
    // for dirichlet estimation
    //protected int[] docLengthCounts; // histogram of document sizes
    //protected int[][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>
    boolean shouldSaveState = false;
    protected FTree[][] trees; //store 
    protected Randoms random;
    protected int threadId = -1;
    protected Queue<FastQDelta> queue;
    private final CyclicBarrier cyclicBarrier;
    protected int MHsteps = 1;
    //protected boolean useCycleProposals = false;
    protected List<Integer> inActiveTopicIndex;

    
    public FastQMVWorkerRunnable(
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
            int[][] tokensPerTopic,
            int startDoc,
            int numDocs,
            FTree[][] trees,
            //boolean useCycleProposals,
            int threadId,
            double[][] p_a, // a for beta prior for modalities correlation
            double[][] p_b, // b for beta prir for modalities correlation
            //            ConcurrentLinkedQueue<FastQDelta> queue, 
            CyclicBarrier cyclicBarrier,
            List<Integer> inActiveTopicIndex
            

    //, FTree betaSmoothingTree
    ) {

        this.data = data;
        this.threadId = threadId;
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
        this.docSmoothingOnlyCumValues = docSmoothingOnlyCumValues;
        this.docSmoothingOnlyMass = docSmoothingOnlyMass;
        this.p_a = p_a;
        this.p_b = p_b;
        this.inActiveTopicIndex = inActiveTopicIndex;
        
    }

    public void setQueue(Queue<FastQDelta> queue) {
        this.queue = queue;
    }
    
    
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

                
                sampleTopicsForOneDoc(doc);
                

            }

            shouldSaveState = false;
            //isFinished = true;
            //logger.info("Worker[" + threadId + "] thread finished");
            queue.add(new FastQDelta(-1, -1, -1, -1, -1, -1));

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
                            double p_wt = (currentTypeTopicCounts[topic] + beta[m]) / (tokensPerTopic[m][topic] + betaSum[m]);

                            
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

                            newTopic = inActiveTopicIndex.get(0);//ThreadLocalRandom.current().nextInt(inActiveTopicIndex.size()));
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
                        queue.add(new FastQDelta(oldTopic, newTopic, type, m, oldTopic == FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC ? 0 : localTopicCounts[m][oldTopic], localTopicCounts[m][newTopic]));
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


    }


}
