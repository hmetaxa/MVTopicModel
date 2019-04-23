package org.madgik.MVTopicModel;

import org.madgik.utils.FastQDelta;
import org.madgik.utils.FTree;
import cc.mallet.types.Dirichlet;
import org.apache.log4j.Logger;
import cc.mallet.util.Randoms;
import static java.lang.Math.log;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.knowceans.util.RandomSamplers;
import org.knowceans.util.Samplers;
import org.knowceans.util.Vectors;

/**
 * Parallel multi-view topic model updater task using FTrees
 *
 * Update thread
 *
 * @author Omiros Metaxas
 */
public class FastQMVWVUpdaterRunnable implements Runnable {

    public static Logger logger = Logger.getLogger("SciTopic");
    protected int[][][] typeTopicCounts; // indexed by <feature index, topic index>
    protected AtomicIntegerArray[] tokensPerTopic; // indexed by <topic index>
    protected FTree[][] trees; //store 
    protected List<BlockingQueue<FastQDelta>> queues;
    protected double[][] alpha;	 // Dirichlet(alpha,alpha,...) is the distribution over topics
    protected double[] alphaSum;
    protected double[] beta;   // Prior on per-topic multinomial distribution over words
    protected double[] betaSum;

    protected double[] tablesCnt; // tables count per modality 
    protected double[] gamma; //gamma per modality 
    protected double gammaRoot = 10; // gammaRoot for all modalities (sumTables cnt)
    protected RandomSamplers samp;

    protected int numTopics;
    public byte numModalities; // Number of modalities
    protected int numTypes[];
    // The max over typeTotals, used for beta[0] optimization
    protected int maxTypeCount[];
    protected Randoms random;
    //protected int[][] docLengthCounts; // histogram of document sizes
    public AtomicIntegerArray[][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>

    protected double[] docSmoothingOnlyMass;
    protected double[][] docSmoothingOnlyCumValues;

    //protected FTree betaSmoothingTree;
    private final CyclicBarrier cyclicBarrier;
    public static final double DEFAULT_BETA = 0.01;
    boolean optimizeParams = false;
    
    protected int threadId = -1;
    protected int nst = -1;
    protected int nut = -1;

    // Optimize gamma[0] hyper params
    protected ConcurrentSkipListSet<Integer>  inActiveTopicIndex; //inactive topic index for all modalities
    private NumberFormat formatter;

    protected double[][] expDotProductValues;  //<topic,token>
    protected double[] sumExpValues; // Partition function values per topic     
    protected double useVectorsLambda = 0; //the weight of the p(w|t) probability based on word / topic emdeddings (it should be 0 either when we don't use vectors or we haven;t  calculated related softmax based probabilities (i.e., during burn in period)

    public FastQMVWVUpdaterRunnable(
            int[][][] typeTopicCounts,
            AtomicIntegerArray[] tokensPerTopic,
            FTree[][] trees,
            //List<ConcurrentLinkedQueue<FastQDelta>> queues, in order not to remain in GC 
            double[][] alpha,
            double[] alphaSum,
            double[] beta,
            double[] betaSum,
            double[] gamma,
            double[] docSmoothingOnlyMass,
            double[][] docSmoothingOnlyCumValues,
            CyclicBarrier cyclicBarrier,
            int numTopics,
            byte numModalities,
            //int[][] docLengthCounts,
            AtomicIntegerArray[][] topicDocCounts,
            int[] numTypes,
            int[] maxTypeCount,
            Randoms random,
            ConcurrentSkipListSet<Integer> inActiveTopicIndex,
            double[][] expDotProductValues, //<topic,token>
            double[] sumExpValues,// Partition function values per topic     
            int threadId, 
            int nst, //numf of sampling threads
            int nut, //num of updater threads 
            List<BlockingQueue<FastQDelta>> queues
    
    ) {

        this.alphaSum = alphaSum;
        this.cyclicBarrier = cyclicBarrier;
        this.alpha = alpha;
        this.beta = beta;
        this.betaSum = betaSum;
        this.gamma = gamma;
        //this.queues = queues;
        this.typeTopicCounts = typeTopicCounts;
        this.tokensPerTopic = tokensPerTopic;
        this.trees = trees;
        this.threadId = threadId;
        this.nst = nst;
        this.nut= nut;
        this.queues = queues; 

        this.numTopics = numTopics;
        this.numModalities = numModalities;
        //this.docLengthCounts = docLengthCounts;
        this.topicDocCounts = topicDocCounts;
        this.numTypes = numTypes;
        this.random = random;
        this.maxTypeCount = maxTypeCount;

        this.docSmoothingOnlyCumValues = docSmoothingOnlyCumValues;
        this.docSmoothingOnlyMass = docSmoothingOnlyMass;
        //this.typeTopicSimilarity = typeTopicSimilarity;

        formatter = NumberFormat.getInstance();
        formatter.setMaximumFractionDigits(5);
        this.inActiveTopicIndex = inActiveTopicIndex;

        this.samp = new RandomSamplers(ThreadLocalRandom.current());

        tablesCnt = new double[numModalities];
        this.expDotProductValues = expDotProductValues;  //<topic,token>
        this.sumExpValues = sumExpValues; // Partition function values per topic     

        //this.betaSmoothingTree = betaSmoothingTree;
        //finishedSamplingTreads = new boolean
    }

    public boolean isFinished = true;

    public void setOptimizeParams(boolean optimizeParams) {
        this.optimizeParams = optimizeParams;
    }

    public void setUseVectorsLambda(double useVectorsLambda) {
        this.useVectorsLambda = useVectorsLambda;
    }

    /*
    public void setQueues(List<BlockingQueue<FastQDelta>> queues) {
        this.queues = queues;
    }
*/
    public void run() {

        Set<Integer> finishedSamplingTreads = new HashSet<Integer>();

        if (!isFinished) {
            System.out.println("already running!");
            return;
        }
        isFinished = false;
        try {
            if (optimizeParams) {
//                optimizeDP();
//                optimizeGamma();
//                optimizeBeta();
//                recalcTrees();
            }

            while (!isFinished) {

                FastQDelta delta;
                int[] currentTypeTopicCounts;
                for (int stId = 0; stId < nst; stId++) {
                    //logger.info("Updater thread: processing queue["+x+"]");
                    while ((delta = queues.get(threadId*nst+stId).poll()) != null) {

                        if (delta.Modality == -1 && delta.NewTopic == -1 && delta.OldTopic == -1 && delta.Type == -1) { // thread x has finished
                            finishedSamplingTreads.add(stId);
                            isFinished = finishedSamplingTreads.size() == nst;

                            //logger.info("Updater thread: worker["+stId+"] has finished");
                            continue;
                        }

                        currentTypeTopicCounts = typeTopicCounts[delta.Modality][delta.Type];

                        if (delta.OldTopic != FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {
                            // Decrement the global topic count totals
                            currentTypeTopicCounts[delta.OldTopic]--;
                            if (currentTypeTopicCounts[delta.OldTopic] < 0) {
                                logger.info("TypeTopicCounts for old Topic " + delta.OldTopic + " below 0");
                            }
                            assert (currentTypeTopicCounts[delta.OldTopic] >= 0) : "TypeTopicCounts for old Topic " + delta.OldTopic + " below 0";
                        }
                        currentTypeTopicCounts[delta.NewTopic]++;

                        if (delta.OldTopic != FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {
                            tokensPerTopic[delta.Modality].decrementAndGet(delta.OldTopic);
                            if (tokensPerTopic[delta.Modality].get(delta.OldTopic) < 0) {
                                logger.info("tokensPerTopic for old Topic " + delta.OldTopic + " below 0");
                            }

                            assert (tokensPerTopic[delta.Modality].get(delta.OldTopic) >= 0) : "tokensPerTopic for old Topic " + delta.OldTopic + " below 0";
                        }

                        tokensPerTopic[delta.Modality].incrementAndGet(delta.NewTopic);

                        if (delta.OldTopic != FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {
                            //update histograms
                            topicDocCounts[delta.Modality][delta.OldTopic].decrementAndGet(delta.DocOldTopicCnt + 1);

                            if (delta.DocOldTopicCnt > 0) {
                                topicDocCounts[delta.Modality][delta.OldTopic].incrementAndGet(delta.DocOldTopicCnt);
                            }
                        }

                        if (delta.DocNewTopicCnt > 1) {
                            topicDocCounts[delta.Modality][delta.NewTopic].decrementAndGet(delta.DocNewTopicCnt - 1);
                        }
                        topicDocCounts[delta.Modality][delta.NewTopic].incrementAndGet(delta.DocNewTopicCnt);

                        //Update tree
//                        if (useCycleProposals) {
//                            trees[delta.Modality][delta.Type].update(delta.OldTopic, ((currentTypeTopicCounts[delta.OldTopic] + beta[delta.Modality]) / (tokensPerTopic[delta.Modality][delta.OldTopic] + betaSum[delta.Modality])));
//                            trees[delta.Modality][delta.Type].update(delta.NewTopic, ((currentTypeTopicCounts[delta.NewTopic] + beta[delta.Modality]) / (tokensPerTopic[delta.Modality][delta.NewTopic] + betaSum[delta.Modality])));
//
//                            //betaSmoothingTree.update(delta.OldTopic, (beta[0] / (tokensPerTopic[delta.OldTopic] + betaSum[0])));
//                            //betaSmoothingTree.update(delta.NewTopic, ( beta[0] / (tokensPerTopic[delta.NewTopic] + betaSum[0])));
//                        } else {
                        if (delta.OldTopic != FastQMVWVParallelTopicModel.UNASSIGNED_TOPIC) {

                            double p_wt = (useVectorsLambda != 0 && delta.Modality == 0)
                                    ? (useVectorsLambda * (expDotProductValues[delta.OldTopic][delta.Type] / 
                                    sumExpValues[delta.OldTopic]) + (1 - useVectorsLambda) * ((currentTypeTopicCounts[delta.OldTopic] 
                                    + beta[delta.Modality]) / (tokensPerTopic[delta.Modality].get(delta.OldTopic) + betaSum[delta.Modality])))
                                    : (currentTypeTopicCounts[delta.OldTopic] + beta[delta.Modality]) / (tokensPerTopic[delta.Modality].get(delta.OldTopic)
                                    + betaSum[delta.Modality]);

                            trees[delta.Modality][delta.Type].update(delta.OldTopic, (gamma[delta.Modality] * alpha[delta.Modality][delta.OldTopic] * p_wt));
                        }
                        double p_wt_newTopic = (useVectorsLambda != 0 && delta.Modality == 0)
                                    ? (useVectorsLambda * (expDotProductValues[delta.NewTopic][delta.Type] 
                                / sumExpValues[delta.NewTopic]) + (1 - useVectorsLambda) * ((currentTypeTopicCounts[delta.NewTopic] 
                                + beta[delta.Modality]) / (tokensPerTopic[delta.Modality].get(delta.NewTopic) + betaSum[delta.Modality])))
                                    : (currentTypeTopicCounts[delta.NewTopic] + beta[delta.Modality]) / (tokensPerTopic[delta.Modality].get(delta.NewTopic) 
                                + betaSum[delta.Modality]);

                        trees[delta.Modality][delta.Type].update(delta.NewTopic, (gamma[delta.Modality] * alpha[delta.Modality][delta.NewTopic] * p_wt_newTopic ));
                        //}

                        if (inActiveTopicIndex.contains(delta.NewTopic)) //new topic
                        {
                            logger.info("New Topic sampled: " + delta.NewTopic);
                            inActiveTopicIndex.remove(new Integer(delta.NewTopic));

                            alpha[delta.Modality][delta.NewTopic] = alpha[delta.Modality][numTopics];
                            //optimizeDP(); in order not to optimize all the time
                        }

                    }

                }

                try {
                    Thread.currentThread().sleep(20);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            }

            try {
                logger.info("Updater thread: ["+threadId+"] has finished");
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

    /*
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
                if (Double.isNaN(betaSum[m])) {
                    betaSum[m] = prevBetaSum;
                }
                beta[m] = betaSum[m] / numTypes[m];
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

        double totaltablesCnt = 0;
        for (Byte m = 0; m < numModalities; m++) {
            totaltablesCnt += tablesCnt[m];

        }

        for (Byte m = 0; m < numModalities; m++) {
            for (int r = 0; r < R; r++) {
                // gamma[0]: root level (Escobar+West95) with n = T
                // (14)
                double eta = samp.randBeta(gammaRoot + 1, totaltablesCnt);
                double bloge = bgamma - log(eta);
                // (13')
                double pie = 1. / (1. + (totaltablesCnt * bloge / (agamma + numTopics - 1)));
                // (13)
                int u = samp.randBernoulli(pie);
                gammaRoot = samp.randGamma(agamma + numTopics - 1 + u, 1. / bloge);

                // for (byte m = 0; m < numModalities; m++) {
                // alpha: document level (Teh+06)
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

                //  }
            }
            logger.info("GammaRoot: " + gammaRoot);
            //for (byte m = 0; m < numModalities; m++) {
            logger.info("Gamma[" + m + "]: " + gamma[m]);
            //}
        }

    }

    private void optimizeDP() {
        double[][] mk = new double[numModalities][numTopics + 1];

        Arrays.fill(tablesCnt, 0);

        for (int t = 0; t < numTopics; t++) {
            inActiveTopicIndex.add(t); //inActive by default and activate if found 
        }

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
            // end outter for loop

            if (!inActiveTopicIndex.isEmpty()) {
                String empty = "";

                for (int i = 0; i < inActiveTopicIndex.size(); i++) {
                    empty += formatter.format(inActiveTopicIndex.get(i)) + " ";
                }
                logger.info("Inactive Topics: " + empty);
            }
            //for (byte m = 0; m < numModalities; m++) {
            //alpha[m].fill(0, numTopics, 0);
            alphaSum[m] = 0;
            mk[m][numTopics] = gammaRoot;
            tablesCnt[m] = Vectors.sum(mk[m]);

            byte numSamples = 10;
            for (int i = 0; i < numSamples; i++) {
                double[] tt = sampleDirichlet(mk[m]);
                // On non parametric with new topic we would have numTopics+1 topics for (int kk = 0; kk <= numTopics; kk++) {
                for (int kk = 0; kk <= numTopics; kk++) {
                    //int k = kactive.get(kk);
                    alpha[m][kk] = tt[kk] / (double) numSamples;
                    alphaSum[m] += alpha[m][kk];
                    //tau.set(k, tt[kk]);
                }
            }

            logger.info("AlphaSum[" + m + "]: " + alphaSum[m]);
            //for (byte m = 0; m < numModalities; m++) {
            String alphaStr = "";
            for (int topic = 0; topic < numTopics; topic++) {
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

            distribution[i] = random.nextGamma(partition[i] * magnitude, 1);
            if (distribution[i] <= 0) {
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
*/

//   
}
