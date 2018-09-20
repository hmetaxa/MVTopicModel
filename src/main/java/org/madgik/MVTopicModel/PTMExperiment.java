package org.madgik.MVTopicModel;

import org.madgik.utils.CsvBuilder;
import org.madgik.utils.HtmlBuilder;
import cc.mallet.util.*;

import cc.mallet.types.*;
import cc.mallet.pipe.*;
import cc.mallet.topics.ParallelTopicModel;

import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.*;
//import java.util.regex.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.madgik.utils.CSV2FeatureSequence;
import org.madgik.utils.FeatureSequenceRemovePlural;
import static org.madgik.utils.Utils.cosineSimilarity;

public class PTMExperiment {

    public enum ExperimentType {

        ACM,
        OAFullGrants,
        OAFETGrants,
        Tender,
        LFR,
        DBLP,
        DBLPNetOnly
    }

    public enum SimilarityType {

        cos,
        Jen_Sha_Div,
        symKL
    }

    public enum Net2BoWType {

        OneWay,
        TwoWay,
        PPR
    }

    public static Logger logger = Logger.getLogger(PTMFlow.class.getName());

    public PTMExperiment() throws IOException {

        int topWords = 20;
        int showTopicsInterval = 50;
        //int topLabels = 10;p

        //int numIndependentTopics = 0;
        double docTopicsThreshold = 0.03;
        int docTopicsMax = -1;
        //boolean ignoreLabels = true;
        //boolean runOnLine = false;

        //boolean calcTokensPerEntity = true;
        int numOfThreads = 2;
        //iMixParallelTopicModel.SkewType skewOn = iMixParallelTopicModel.SkewType.None;
        //boolean ignoreSkewness = true;
        byte numModalities = 2;
        int numTopics = 6;
        //int maxNumTopics = 500;
        int numIterations = 700; //Max 2000
        int numChars = 20000;
        //int independentIterations = 0;
        int burnIn = 50;
        int optimizeInterval = 20;
        ExperimentType experimentType = ExperimentType.Tender;
        String experimentSubType = "";
        int pruneCnt = 20; //Red,,,,,,,,,,,,,,,,,,,,,,,,,uce features to those that occur more than N times
        int pruneLblCnt = 4;
        double pruneMaxPerc = 1;//Remove features that occur in more than (X*100)% of documents. 0.05 is equivalent to IDF of 3.0.
        double pruneMinPerc = 1;//Remove features that occur in more than (X*100)% of documents. 0.05 is equivalent to IDF of 3.0.
        SimilarityType similarityType = SimilarityType.cos; //Cosine 1 jensenShannonDivergence 2 symmetric KLP
        boolean ACMAuthorSimilarity = true;
        boolean ubuntu = false;

        boolean calcEntitySimilarities = false;
        boolean calcTopicSimilarities = false;
        boolean calcPPRSimilarities = false;
        boolean runTopicModelling = true;
        boolean runOrigParallelModel = false;
        boolean runWordEmbeddings = false;
        boolean useTypeVectors = false;
        boolean trainTypeVectors = false;
        boolean findKeyPhrases = false;
        double useTypeVectorsProb = 0.6;
        Net2BoWType PPRenabled = Net2BoWType.PPR;

        int vectorSize = 200;
        //vectorSize[0] = 200;
//boolean runParametric = true;
//
//        try {
//
//            double[] temp = {0.3, 1.5, 0.4, 0.3};
//            FTree tree = new FTree(temp);
//
//            int tmp = tree.sample(2.1 / tree.tree[1]);
//            logger.info("FTree sample 2.1 (2):" + t);
//
//            double tmp2 = tree.getComponent(0);
//            logger.info("FTree getComponent(0):" + tmp2);
//
//            tmp2 = tree.getComponent(3);
//            logger.info("FTree getComponent(3):" + tmp2);
//
//            tree.update(2, 1.4);
//
//            tmp = tree.sample(3.19 /tree.tree[1]);
//            logger.info("FTree sample 3.19 (2):" + tmp);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //String addedExpId = (experimentType == ExperimentType.ACM ? (ACMAuthorSimilarity ? "Author" : "Category") : "");
        String experimentId = experimentType.toString() + "_" + numTopics + "T_"
                + numIterations + "IT_" + numChars + "CHRs_" + pruneCnt + "_" + pruneLblCnt + "PRN" + burnIn + "B_" + numModalities + "M_" + numOfThreads + "TH_" + similarityType.toString() + (useTypeVectors ? "WV" : "") + PPRenabled.name() + "_" + experimentSubType; // + "_" + skewOn.toString();

        //experimentId = "HEALTHTender_400T_1000IT_6000CHRs_100B_2M_cos";
        String experimentDescription = experimentId + ": \n";

        String SQLLitedb = "";//"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
        //File dictPath = null;

        String dbFilename = "";
        String dictDir = "";
        if (experimentType == ExperimentType.ACM) {
            dbFilename = "PTMDB_ACM2016.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/ACM/";
            } else {
                dictDir = "C:\\projects\\Datasets\\ACM\\";
            }
        } else if (experimentType == ExperimentType.Tender) {
            dbFilename = "PTM_Tender.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/PubMed/";
            } else {
                dictDir = "C:\\projects\\Datasets\\Tender\\";
            }
        } else if (experimentType == ExperimentType.OAFullGrants) {
            dbFilename = "PTMDB_OpenAIRE.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/OpenAIRE/";
            } else {
                dictDir = "C:\\projects\\Datasets\\OpenAIRE\\";
            }
        } else if (experimentType == ExperimentType.LFR) {
            dbFilename = "LFRNet.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/OverlappingNets/";
            } else {
                dictDir = "C:\\Projects\\datasets\\OverlappingNets\\LFR\\100K\\NoNoise\\";
            }
        } else if (experimentType == ExperimentType.DBLP || experimentType == ExperimentType.DBLPNetOnly) {
            dbFilename = "DBLP4Areas.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/DBLP/";
            } else {
                dictDir = "C:\\Projects\\datasets\\DBLP\\";
            }
        }

        SQLLitedb = "jdbc:sqlite:" + dictDir + dbFilename + "?journal_mode=WAL&synchronous=OFF";

//        if (dictDir != "") {
//            dictPath = new File(dictDir);
//            dictPath.mkdir();
//
//        }
        Connection connection = null;

        //FindGroundTruthCommunities(SQLLitedb, experimentId);
//        if (experimentType == ExperimentType.LFR) {
//            FindGroundTruthCommunities(SQLLitedb, experimentId);
//        }
        //createRefACMTables(SQLLitedb);
        // create a database connection
        //Reader fileReader = new InputStreamReader(new FileInputStream(new File(args[0])), "UTF-8");
        //instances.addThruPipe(new CsvIterator (fileReader, Pattern.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"),
        //3, 2, 1)); // data, label, name fields
        if (findKeyPhrases) {
            FindKeyPhrasesPerTopic(SQLLitedb, experimentId, "openNLP");
        }

        if (runWordEmbeddings) {
            logger.info(" calc word embeddings starting");
            InstanceList[] instances = GenerateAlphabets(SQLLitedb, experimentType, dictDir, numModalities, pruneCnt,
                    pruneLblCnt, pruneMaxPerc, pruneMinPerc, numChars, PPRenabled,
                    experimentSubType, experimentType == ExperimentType.LFR || experimentType == ExperimentType.DBLP || experimentType == ExperimentType.DBLPNetOnly);
            logger.info(" instances added through pipe");

            //int numDimensions = 50;
            int windowSizeOption = 5;
            int numSamples = 5;
            WordEmbeddings matrix = new WordEmbeddings(instances[0].getDataAlphabet(), vectorSize, windowSizeOption);
            matrix.setQueryWord("mining");
            matrix.countWords(instances[0], 0.0001); //Sampling factor : "Down-sample words that account for more than ~2.5x this proportion or the corpus."
            matrix.train(instances[0], numOfThreads, numSamples, 5); //iterations

            //PrintWriter out = new PrintWriter("vectors.txt");
            //matrix.write(out);
            //out.close();
            matrix.write(SQLLitedb, 0);
            logger.info(" calc word embeddings ended");
        }

        if (runTopicModelling) {

            //Create vocabularies for the whole corpus
            //search for file first
            //String txtAlphabetFile = dictDir + File.separator + "dict[0].txt";
            //Alphabet[] alphabets = new Alphabet[numModalities];
            //String outputDir = dictDir + experimentId;
            //File outPath = new File(outputDir);
            //outPath.mkdir();
            //String stateFile = outputDir + File.separator + "output_state";
            //String outputDocTopicsFile = outputDir + File.separator + "output_doc_topics.csv";
            //String outputTopicPhraseXMLReport = outputDir + File.separator + "topicPhraseXMLReport.xml";
            //String topicKeysFile = outputDir + File.separator + "output_topic_keys.csv";
            //String topicWordWeightsFile = outputDir + File.separator + "topicWordWeightsFile.csv";
            //String stateFileZip = outputDir + File.separator + "output_state.gz";
            //String modelEvaluationFile = outputDir + File.separator + "model_evaluation.txt";
            //String modelDiagnosticsFile = outputDir + File.separator + "model_diagnostics.xml";
            String batchId = "-1";
            InstanceList[] instances = GenerateAlphabets(SQLLitedb, experimentType, dictDir, numModalities, pruneCnt,
                    pruneLblCnt, pruneMaxPerc, pruneMinPerc, numChars, PPRenabled, experimentSubType,
                    experimentType == ExperimentType.LFR || experimentType == ExperimentType.DBLP || experimentType == ExperimentType.DBLPNetOnly);
            logger.info(" instances added through pipe");

            if (runOrigParallelModel) {
                ParallelTopicModel modelOrig = new ParallelTopicModel(numTopics, numTopics * 0.01, 0.01);

                modelOrig.addInstances(instances[0]);

                // Use two parallel samplers, which each look at one half the corpus and combine
                //  statistics after every iteration.
                modelOrig.setNumThreads(numOfThreads + 1);
                // Run the model for 50 iterations and stop (this is for testing only, 
                //  for real applications, use 1000 to 2000 iterations)
                modelOrig.setNumIterations(numIterations);
                modelOrig.optimizeInterval = optimizeInterval;
                modelOrig.burninPeriod = burnIn;
                modelOrig.setTopicDisplay(showTopicsInterval, topWords);
                //model.optimizeInterval = 0;
                //model.burninPeriod = 0;
                //model.saveModelInterval=250;
                modelOrig.estimate();
            } else {
                double beta = 0.01;
                double[] betaMod = new double[numModalities];
                Arrays.fill(betaMod, 0.01);
                boolean useCycleProposals = false;
                double alpha = 0.1;

                double[] alphaSum = new double[numModalities];
                Arrays.fill(alphaSum, 1);

                double[] gamma = new double[numModalities];
                Arrays.fill(gamma, 1);

                //double gammaRoot = 4;
                FastQMVWVParallelTopicModel model = new FastQMVWVParallelTopicModel(numTopics, numModalities, alpha, beta, useCycleProposals, SQLLitedb, useTypeVectors, useTypeVectorsProb, trainTypeVectors);

                model.CreateTables(SQLLitedb, experimentId);

                // ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);
                model.setNumIterations(numIterations);
                model.setTopicDisplay(showTopicsInterval, topWords);
                // model.setIndependentIterations(independentIterations);
                model.optimizeInterval = optimizeInterval;
                model.burninPeriod = burnIn;
                model.setNumThreads(numOfThreads);

                model.addInstances(instances, batchId, vectorSize, "");//trainingInstances);//instances);
                logger.info(" instances added");

                //model.readWordVectorsDB(SQLLitedb, vectorSize);
                model.estimate();
                logger.info("Model estimated");

                model.saveTopics(SQLLitedb, experimentId, batchId);
                logger.info("Topics Saved");

                PrintWriter outState = null;// new PrintWriter(new FileWriter((new File(outputDocTopicsFile))));

                model.printDocumentTopics(outState, docTopicsThreshold, docTopicsMax, SQLLitedb, experimentId, batchId);

                if (outState != null) {
                    outState.close();
                }

                logger.info("printDocumentTopics finished");

                logger.info("Model Id: \n" + experimentId);

                logger.info("Model Metadata: \n" + model.getExpMetadata());

                //if (modelEvaluationFile != null) {
                try {

//                ObjectOutputStream oos =
//                        new ObjectOutputStream(new FileOutputStream(modelEvaluationFile));
//                oos.writeObject(model.getProbEstimator());
//                oos.close();
//                
                    //PrintStream docProbabilityStream = null;
                    //docProbabilityStream = new PrintStream(modelEvaluationFile);
//TODO...
                    double perplexity = 0;
//                        if (splitCorpus) {
//                            perplexity = model.getProbEstimator().evaluateLeftToRight(testInstances[0], 10, false, docProbabilityStream);
//                        }
                    //  System.out.println("perplexity for the test set=" + perplexity);
                    //logger.info("perplexity calculation finished");
                    //iMixLDATopicModelDiagnostics diagnostics = new iMixLDATopicModelDiagnostics(model, topWords);
                    //MixLDATopicModelDiagnostics diagnostics = new MixLDATopicModelDiagnostics(model, topWords);

                    FastQMVWVTopicModelDiagnostics diagnostics = new FastQMVWVTopicModelDiagnostics(model, topWords);
                    diagnostics.saveToDB(SQLLitedb, experimentId, perplexity, batchId);
                    logger.info("full diagnostics calculation finished");

                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }

                //  }
                //eee
                //    }
                experimentDescription = "Multi View Topic Modeling Analysis on ACM corpus";
                model.saveExperiment(SQLLitedb, experimentId, experimentDescription);

                if (experimentType == ExperimentType.LFR) {
                    FindGroundTruthCommunities(SQLLitedb, experimentId);
                }

//                PrintWriter outXMLPhrase = new PrintWriter(new FileWriter((new File(outputTopicPhraseXMLReport))));
//                model.topicPhraseXMLReport(outXMLPhrase, topWords);
//                outXMLPhrase.close();
//                logger.info("topicPhraseXML report finished");
                logger.info("Insert default topic descriptions");

                try {
                    // create a database connection
                    //connection = DriverManager.getConnection(SQLLitedb);

                    String insertTopicDescriptionSql = "INSERT into TopicDescription (Title, Category, TopicId , VisibilityIndex, ExperimentId )\n"
                            + "select substr(GROUP_CONCAT(Item),1,100), '' , topicId , 1, '" + experimentId + "' \n"
                            + "from  TopicDescriptionView\n"
                            + " where experimentID = '" + experimentId + "' \n"
                            + " GROUP BY TopicID";
                    connection = DriverManager.getConnection(SQLLitedb);
                    Statement statement = connection.createStatement();
                    statement.executeUpdate(insertTopicDescriptionSql);
                    //ResultSet rs = statement.executeQuery(sql);

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
        }
        if (calcEntitySimilarities) {

            calcSimilarities(SQLLitedb, experimentType, experimentId, ACMAuthorSimilarity, similarityType, numTopics);

        }

        if (calcTopicSimilarities) {
            CalcTopicSimilarities(SQLLitedb);
        }

        if (calcPPRSimilarities) {
            calcPPRSimilarities(SQLLitedb);
        }

//        if (modelDiagnosticsFile
//                != null) {
//            PrintWriter out = new PrintWriter(modelDiagnosticsFile);
//            MixTopicModelDiagnostics diagnostics = new MixTopicModelDiagnostics(model, topWords, perplexity);
//            diagnostics.saveToDB(SQLLitedb, experimentId);
//            out.println(diagnostics.toXML()); //preferable than XML???
//            out.close();
//        }
        //If any value in <tt>p2</tt> is <tt>0.0</tt> then the KL-divergence
        //double a = Maths.klDivergence();
        //model.printTypeTopicCounts(new File (wordTopicCountsFile.value));
        // Show the words and topics in the first instance
        // The data alphabet maps word IDs to strings
        /*   Alphabet dataAlphabet = instances.getDataAlphabet();

         FeatureSequence tokens = (FeatureSequence) model.getData().get(0).instance.getData();
         LabelSequence topics = model.getData().get(0).topicSequence;

         Formatter out = new Formatter(new StringBuilder(), Locale.US);
         for (int posit= 0; position < tokens.getLength(); position++) {
         out.format("%s-%d ", dataAlphabet.lookupObject(tokens.getIndexAtPosition(position)), topics.getIndexAtPosition(position));
         }
         System.out.println(out);

         // Estimate the topic distribution of the first instance, 
         //  given the current Gibbs state.
         double[] topicDistribution = model.getTopicProbabilities(0);

         // Get an array of sorted sets of word ID/count pairs
         ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();

         // Show top 5 words in topics with proportions for the first document
         for (int topic = 0; topic < numTopics; topic++) {
         Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();

         out = new Formatter(new StringBuilder(), Locale.US);
         out.format("%d\t%.3f\t", topic, topicDistribution[topic]);
         int rank = 0;
         while (iterator.hasNext() && rank < 5) {
         IDSorter idCountPair = iterator.next();
         out.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight());
         rank++;
         }
         System.out.println(out);
         }

         // Create a new instance with high probability of topic 0
         StringBuilder topicZeroText = new StringBuilder();
         Iterator<IDSorter> iterator = topicSortedWords.get(0).iterator();

         int rank = 0;
         while (iterator.hasNext() && rank < 5) {
         IDSorter idCountPair = iterator.next();
         topicZeroText.append(dataAlphabet.lookupObject(idCountPair.getID()) + " ");
         rank++;
         }

         // Create a new instance named "test instance" with empty target and source fields.
         InstanceList testing = new InstanceList(instances.getPipe());
         testing.addThruPipe(new Instance(topicZeroText.toString(), null, "test instance", null));

         TopicInferencer inferencer = model.getInferencer();
         double[] testProbabilities = inferencer.getSampledDistribution(testing.get(0), 10, 1, 5);
         System.out.println("0\t" + testProbabilities[0]);
         */
    }

    private void SaveTopKTokensPerEntity(int K, boolean TfIDFweighting, InstanceList instances) {

    }

    private void FindKeyPhrasesPerTopic(String SQLLiteDB, String experimentId, String tagger) {
        //for default lexicon POS tags
        //Configuration.setTaggerType("default"); 
        if (tagger == "openNLP") {
            // for openNLP POS tagger
            Configuration.setTaggerType(tagger);
            //for Stanford POS tagger
            // if tagger type is "openNLP" then give the openNLP POS tagger path
            Configuration.setModelFileLocation("model/openNLP/en-pos-maxent.bin");
        } else if (tagger == "stanford") {
            Configuration.setTaggerType("stanford");
            Configuration.setModelFileLocation("model/stanford/english-left3words-distsim.tagger");

        }

        Configuration.setSingleStrength(4);
        Configuration.setNoLimitStrength(2);
        // if tagger type is "default" then give the default POS lexicon file
        //Configuration.setModelFileLocation("model/default/english-lexicon.txt");
        // if tagger type is "stanford "
        //Configuration.setModelFileLocation("model/stanford/english-left3words-distsim.tagger");

        TermsExtractor termExtractor = new TermsExtractor();
        TermDocument topiaDoc = new TermDocument();

        StringBuffer stringBuffer = new StringBuffer();

        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLLitedb);
            connection = DriverManager.getConnection(SQLLiteDB);
            Statement statement = connection.createStatement();

            logger.info("Finding key phrases calculation started");

            String sql = "select pubtopic.TopicId, publication.title, publication.abstract from \n"
                    + "PubTopic\n"
                    + "inner join publication on PubTopic.pubId= publication.PubId and pubTOpic.Weight>0.55 \n"
                    + "where experimentId='" + experimentId + "' \n"
                    + "order by pubtopic.topicid, weight desc";

            ResultSet rs = statement.executeQuery(sql);

            HashMap<Integer, Map<String, ArrayList<Integer>>> topicTitles = null;

            topicTitles = new HashMap<Integer, Map<String, ArrayList<Integer>>>();

            Integer topicId = -1;

            while (rs.next()) {

                int newTopicId = rs.getInt("TopicId");

                if (newTopicId != topicId && topicId != -1) {

                    logger.info("Finding key phrases for topic " + topicId);
                    topiaDoc = termExtractor.extractTerms(stringBuffer.toString());
                    topicTitles.put(topicId, topiaDoc.getFinalFilteredTerms());
                    stringBuffer = new StringBuffer();
                }
                stringBuffer.append(rs.getString("title").replace('-', ' ').toLowerCase() + "\n");
                //stringBuffer.append(rs.getString("abstract").replace('-', ' ').toLowerCase() + "\n");
                topicId = newTopicId;

            }

            logger.info("Finding key phrases for topic " + topicId);
            topiaDoc = termExtractor.extractTerms(stringBuffer.toString());
            topicTitles.put(topicId, topiaDoc.getFinalFilteredTerms());

            statement.executeUpdate("create table if not exists TopicKeyPhrase ( TopicId Integer, Tagger TEXT, Phrase Text, Count Integer, WordsNum Integer, Weight double, ExperimentId TEXT) ");
            String deleteSQL = String.format("Delete from TopicKeyPhrase WHERE ExperimentId='" + experimentId + "' AND Tagger ='" + tagger + "'");
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            sql = "insert into TopicKeyPhrase values(?,?,?,?,?,?,?);";

            logger.info("Saving key phrases....");
            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (Integer tmpTopicId : topicTitles.keySet()) {
                    //boolean startComparison = false;fuyhgjlkfdytrdfuikol
                    Map<String, ArrayList<Integer>> extractedPhrases = topicTitles.get(tmpTopicId);
                    for (String phrase : extractedPhrases.keySet()) {

                        bulkInsert.setInt(1, tmpTopicId);
                        bulkInsert.setString(2, tagger);
                        bulkInsert.setString(3, phrase);
                        bulkInsert.setInt(4, extractedPhrases.get(phrase).get(0));
                        bulkInsert.setInt(5, extractedPhrases.get(phrase).get(1));
                        bulkInsert.setDouble(6, 0);
                        bulkInsert.setString(7, experimentId);

                        bulkInsert.executeUpdate();
                    }

                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert topicPhrases");
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

        logger.info("Finding Key phrases finished");

    }

    private void TfIdfWeighting(InstanceList instances, String SQLLiteDB, String experimentId, int itemType) {

        int N = instances.size();

        Alphabet alphabet = instances.getDataAlphabet();
        Object[] tokens = alphabet.toArray();
        System.out.println("# Number of dimensions: " + tokens.length);
        // determine document frequency for each term
        int[] df = new int[tokens.length];
        for (Instance instance : instances) {
            FeatureVector fv = new FeatureVector((FeatureSequence) instance.getData());
            int[] indices = fv.getIndices();
            for (int index : indices) {
                df[index]++;
            }
        }

        // determine document length for each document
        int[] lend = new int[N];
        double lenavg = 0;
        for (int i = 0; i < N; i++) {
            Instance instance = instances.get(i);
            FeatureVector fv = new FeatureVector((FeatureSequence) instance.getData());
            int[] indices = fv.getIndices();
            double length = 0.0;
            for (int index : indices) {
                length += fv.value(index);
            }
            lend[i] = (int) length;
            lenavg += length;
        }
        if (N > 1) {
            lenavg /= (double) N;
        }

        Connection connection = null;
        Statement statement = null;
        PreparedStatement bulkInsert = null;

        try {
            // create a database connection
            if (!SQLLiteDB.isEmpty()) {
                connection = DriverManager.getConnection(SQLLiteDB);
                statement = connection.createStatement();
                statement.executeUpdate("create table if not exists TokensPerEntity (EntityId nvarchar(100), ItemType int, Token nvarchar(100), Counts double, TFIDFCounts double, ExperimentId nvarchar(50)) ");

                statement.executeUpdate("create Index if not exists IX_TokensPerEntity_Entity_Counts ON TokensPerEntity ( EntityId, ExperimentId, ItemType, Counts DESC, TFIDFCounts DESC, Token)");
                statement.executeUpdate("create Index if not exists IX_TokensPerEntity_Entity_TFIDFCounts ON TokensPerEntity ( EntityId, ExperimentId, ItemType,  TFIDFCounts DESC, Counts DESC, Token)");

                statement.executeUpdate("create View if not exists TokensPerEntityView AS select rv1.EntityId, rv1.ItemType, rv1.Token, rv1.Counts, rv1.TFIDFCounts, rv1.ExperimentId \n"
                        + "FROM TokensPerEntity rv1\n"
                        + "WHERE Token in\n"
                        + "(\n"
                        + "SELECT Token\n"
                        + "FROM TokensPerEntity rv2\n"
                        + "WHERE EntityId = rv1.EntityId AND Counts>2 AND ItemType=rv1.ItemType AND ExperimentId=rv1.ExperimentId \n"
                        + "ORDER BY\n"
                        + "TFIDFCounts DESC\n"
                        + "LIMIT 20\n"
                        + ")");

                String deleteSQL = String.format("Delete from TokensPerEntity where  ExperimentId = '%s' and itemtype= %d", experimentId, itemType);
                statement.executeUpdate(deleteSQL);

                String sql = "insert into TokensPerEntity values(?,?,?,?,?,?);";

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (int i = 0; i < N; i++) {
                    Instance instance = instances.get(i);

                    FeatureVector fv = new FeatureVector((FeatureSequence) instance.getData());
                    int[] indices = fv.getIndices();
                    for (int index : indices) {
                        double tf = fv.value(index);
                        double tfcomp = tf / (tf + 0.5 + 1.5 * (double) lend[i] / lenavg);
                        double idfcomp = Math.log((double) N / (double) df[index]) / Math.log(N + 1);
                        double tfIdf = tfcomp * idfcomp;
                        fv.setValue(index, tfIdf);
                        String token = fv.getAlphabet().lookupObject(index).toString();

                        bulkInsert.setString(1, instance.getName().toString());
                        bulkInsert.setInt(2, itemType);
                        bulkInsert.setString(3, token);
                        bulkInsert.setDouble(4, tf);
                        bulkInsert.setDouble(5, tfIdf);
                        bulkInsert.setString(6, experimentId);

                        bulkInsert.executeUpdate();
                    }
                }

                connection.commit();
            }
        } catch (SQLException e) {

            if (connection != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    System.err.print("Error in insert TokensPerEntity");
                }
            }
        } finally {
            try {
                if (bulkInsert != null) {
                    bulkInsert.close();
                }
                connection.setAutoCommit(true);
            } catch (SQLException excep) {
                System.err.print("Error in insert TokensPerEntity");
            }
        }

        //TODO: Sort Feature Vector Values
        // FeatureVector.toSimpFilefff
    }

    private void GenerateStoplist(SimpleTokenizer prunedTokenizer, ArrayList<Instance> instanceBuffer, int pruneCount, double docProportionMinCutoff, double docProportionMaxCutoff, boolean preserveCase)
            throws IOException {

        //SimpleTokenizer st = new SimpleTokenizer(new File("stoplists/en.txt"));
        ArrayList<Instance> input = new ArrayList<Instance>();
        for (Instance instance : instanceBuffer) {
            input.add((Instance) instance.clone());
        }

        ArrayList<Pipe> pipes = new ArrayList<Pipe>();
        Alphabet alphabet = new Alphabet();

        CharSequenceLowercase csl = new CharSequenceLowercase();
        //prunedTokenizer = st.deepClone();
        SimpleTokenizer st = prunedTokenizer.deepClone();
        StringList2FeatureSequence sl2fs = new StringList2FeatureSequence(alphabet);
        FeatureCountPipe featureCounter = new FeatureCountPipe(alphabet, null);
        FeatureDocFreqPipe docCounter = new FeatureDocFreqPipe(alphabet, null);

        pipes.add(new Input2CharSequence());

        if (!preserveCase) {
            pipes.add(csl);
        }
        pipes.add(st);
        pipes.add(sl2fs);
        if (pruneCount > 0) {
            pipes.add(featureCounter);
        }
        if (docProportionMaxCutoff < 1.0) {
            //if (docProportionMaxCutoff < 1.0 || docProportionMinCutoff > 0) {
            pipes.add(docCounter);
        }

        Pipe serialPipe = new SerialPipes(pipes);
        Iterator<Instance> iterator = serialPipe.newIteratorFrom(input.iterator());

        int count = 0;

        // We aren't really interested in the instance itself,
        //  just the total feature counts.
        while (iterator.hasNext()) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
            }
            iterator.next();
        }

        Iterator<String> wordIter = alphabet.iterator();
        while (wordIter.hasNext()) {
            String word = (String) wordIter.next();

            if (!word.matches("^(?!.*(-[^-]*-|_[^_]*_))[A-Za-z0-9][\\w-]*[A-Za-z0-9]$") || word.contains("cid") || word.contains("italic") || word.contains("null") || word.contains("usepackage") || word.contains("fig")) {
                prunedTokenizer.stop(word);
            }
        }

        prunedTokenizer.stop("tion");
        prunedTokenizer.stop("ing");
        prunedTokenizer.stop("ment");
        prunedTokenizer.stop("ytem");
        prunedTokenizer.stop("wth");
        prunedTokenizer.stop("whch");
        prunedTokenizer.stop("nfrmatn");
        prunedTokenizer.stop("uer");
        prunedTokenizer.stop("ther");
        prunedTokenizer.stop("frm");
        prunedTokenizer.stop("hypermeda");
        prunedTokenizer.stop("anuae");
        prunedTokenizer.stop("dcument");
        prunedTokenizer.stop("tudent");
        prunedTokenizer.stop("appcatn");
        prunedTokenizer.stop("tructure");
        prunedTokenizer.stop("prram");
        prunedTokenizer.stop("den");
        prunedTokenizer.stop("aed");
        prunedTokenizer.stop("cmputer");
        prunedTokenizer.stop("prram");

        prunedTokenizer.stop("mre");
        prunedTokenizer.stop("cence");
        prunedTokenizer.stop("tures");
        prunedTokenizer.stop("ture");
        prunedTokenizer.stop("ments");
        prunedTokenizer.stop("cations");
        prunedTokenizer.stop("tems");
        prunedTokenizer.stop("tem");
        prunedTokenizer.stop("tional");
        prunedTokenizer.stop("ity");
        prunedTokenizer.stop("ware");
        prunedTokenizer.stop("opment");
        prunedTokenizer.stop("guage");
        prunedTokenizer.stop("niques");

        if (pruneCount > 0) {
            featureCounter.addPrunedWordsToStoplist(prunedTokenizer, pruneCount);
        }
        if (docProportionMaxCutoff < 1.0) {
            docCounter.addPrunedWordsToStoplist(prunedTokenizer, docProportionMaxCutoff);
        }

//        if (pruneCount > 0) {
//            featureCounter.addPrunedWordsToStoplist(prunedTokenizer, pruneCount);
//        }
//        if (docProportionMaxCutoff < 1.0 || docProportionMinCutoff > 0) {
//            docCounter.addPrunedWordsToStoplist(prunedTokenizer, docProportionMaxCutoff, docProportionMinCutoff);
//        }
    }

    private void outputCsvFiles(String outputDir, Boolean htmlOutputFlag, String inputDir, int numTopics, String stateFile, String outputDocTopicsFile, String topicKeysFile) {

        CsvBuilder cb = new CsvBuilder();
        cb.createCsvFiles(numTopics, outputDir, stateFile, outputDocTopicsFile, topicKeysFile);

        if (htmlOutputFlag) {
            HtmlBuilder hb = new HtmlBuilder(cb.getNtd(), new File(inputDir));
            hb.createHtmlFiles(new File(outputDir));
        }
        //clearExtrafiles(outputDir);
    }

    private void clearExtrafiles(String outputDir) {
        String[] fileNames = {"topic-input.mallet", "output_topic_keys.csv", "output_state.gz",
            "output_doc_topics.csv", "output_state"};
        for (String f : fileNames) {
            if (!(new File(outputDir, f).canWrite())) {
                System.out.println(f);
            }
            Boolean b = new File(outputDir, f).delete();

        }

    }

    public void createCitationGraphFile(String outputCsv, String SQLLitedb) {
        //String SQLLitedb = "jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";

        Connection connection = null;
        try {

            FileWriter fwrite = new FileWriter(outputCsv);
            BufferedWriter out = new BufferedWriter(fwrite);
            String header = "# DBLP citation graph \n"
                    + "# fromNodeId, toNodeId \n";
            out.write(header);

            connection = DriverManager.getConnection(SQLLitedb);

            String sql = "select id, ref_id from papers where ref_num >0 ";
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                // read the result set
                int Id = rs.getInt("Id");
                String citationNums = rs.getString("ref_id");

                String csvLine = "";//Id + "\t" + citationNums;

                String[] str = citationNums.split("\t");
                for (int i = 0; i < str.length - 1; i++) {
                    csvLine = Id + "\t" + str[i];
                    out.write(csvLine + "\n");
                }

            }
            out.flush();
        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.println("File input error");
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

    private class TopicVector {

        public int TopicId;
        public String ExperimentId;
        public double[] Vector;
    }

    public boolean isAlphanumeric(String str) {

        return str.matches("^(?!.*(-[^-]*-|_[^_]*_))[A-Za-z0-9][\\w-]*[A-Za-z0-9]$");
        //Pattern p = Pattern.compile("[^a-zA-Z0-9]+$");

    }

    public void CalcEntityTopicDistributionsAndTrends(String SQLLitedb, String experimentId) {
        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            logger.info("Calc topic Entity Topic Distributions and Trends started");

            String deleteSQL = String.format("Delete from EntityTopicDistribution here ExperimentId= '%s'", experimentId);
            statement.executeUpdate(deleteSQL);

            logger.info("Insert Full Topic Distribution ");

            String SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "select '',  PubTopic.TopicId, '', 'Corpus', round(sum(weight)/SumTopicWeightView.SumWeight, 5) as NormWeight, PubTopic.ExperimentId\n"
                    + "from PubTopic\n"
                    + "INNER JOIN (SELECT  sum(weight) AS SumWeight, ExperimentId\n"
                    + "FROM PubTopic\n"
                    + "Where PubTopic.weight>0.1 \n"
                    + " and PubTopic.ExperimentId='" + experimentId + "'  \n"
                    + "GROUP BY  ExperimentId) SumTopicWeightView on SumTopicWeightView.ExperimentId= PubTopic.ExperimentId\n"
                    + "group By PubTopic.TopicId, PubTopic.ExperimentId, SumTopicWeightView.SumWeight\n"
                    + "Order by  NormWeight Desc";

            statement.executeUpdate(SQLstr);

            logger.info("Trend Topic distribution for the whole coprus");

            SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "select Publication.BatchId,  PubTopic.TopicId, '', 'CorpusTrend', \n"
                    + "round(sum(weight)/SumTopicWeightPerBatchView.BatchSumWeight,5) as NormWeight,  PubTopic.ExperimentId\n"
                    + "from PubTopic\n"
                    + "Inner Join Publication on PubTopic.PubId= Publication.PubId and PubTopic.weight>0.1\n"
                    + "INNER JOIN (SELECT Publication.BatchId, sum(weight) AS BatchSumWeight, ExperimentId\n"
                    + "FROM PubTopic\n"
                    + "INNER JOIN Publication ON PubTopic.PubId= Publication.PubId AND\n"
                    + "PubTopic.weight>0.1\n "
                    + "and PubTopic.ExperimentId='" + experimentId + "'   \n"
                    + "GROUP BY Publication.BatchId, ExperimentId) SumTopicWeightPerBatchView on SumTopicWeightPerBatchView.BatchId = Publication.BatchId and SumTopicWeightPerBatchView.ExperimentId= PubTopic.ExperimentId\n"
                    + "group By Publication.BatchId,SumTopicWeightPerBatchView.BatchSumWeight, PubTopic.TopicId, PubTopic.ExperimentId\n"
                    + "Order by Publication.BatchId,   NormWeight Desc";

            statement.executeUpdate(SQLstr);
            logger.info("Project Topic distribution");

            SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "SELECT '', PubTopic.TopicId, PubProject.ProjectId,'Project',\n"
                    + "           round(sum(PubTopic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                    + "             PubTopic.ExperimentId\n"
                    + "      FROM PubTopic\n"
                    + "      INNER JOIN  PubProject ON PubTopic.PubId = PubProject.PubId AND PubTopic.weight > 0.1\n"
                    + "      and  PubTopic.ExperimentId='" + experimentId + "' \n"
                    + "           INNER JOIN (SELECT PubProject.ProjectId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                    + "           FROM PubTopic\n"
                    + "           INNER JOIN   PubProject ON PubTopic.PubId = PubProject.PubId AND  PubTopic.weight > 0.1\n"
                    + "           GROUP BY  ExperimentId,PubProject.ProjectId)\n"
                    + "           SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.ProjectId = PubProject.ProjectId AND \n"
                    + "                                           SumTopicWeightPerProjectView.ExperimentId = PubTopic.ExperimentId                                            \n"
                    + "     GROUP BY PubProject.ProjectId,\n"
                    + "              SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                    + "              PubTopic.TopicId,\n"
                    + "              PubTopic.ExperimentId\n"
                    + "              order by  PubTopic.ExperimentId, PubProject.ProjectId, NormWeight Desc,PubTopic.ExperimentId";

            statement.executeUpdate(SQLstr);

            logger.info("Funder Topic distribution");
            SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "SELECT '', PubTopic.TopicId, Project.funder,'Funder',\n"
                    + "           round(sum(PubTopic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                    + "             PubTopic.ExperimentId\n"
                    + "      FROM PubTopic\n"
                    + "      INNER JOIN  PubProject ON PubTopic.PubId = PubProject.PubId AND PubTopic.weight > 0.1\n"
                    + "      and  PubTopic.ExperimentId='" + experimentId + "' \n"
                    + "      INNER JOIN  Project ON PubProject.ProjectId = Project.ProjectId \n"
                    + "           INNER JOIN (SELECT Project.funder, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                    + "           FROM PubTopic\n"
                    + "           INNER JOIN   PubProject ON PubTopic.PubId = PubProject.PubId AND  PubTopic.weight > 0.1\n"
                    + "           INNER JOIN  Project ON PubProject.ProjectId = Project.ProjectId \n"
                    + "           GROUP BY  ExperimentId,Project.funder)\n"
                    + "           SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.funder = Project.funder AND \n"
                    + "                                           SumTopicWeightPerProjectView.ExperimentId = PubTopic.ExperimentId                                            \n"
                    + "     GROUP BY Project.funder,\n"
                    + "              SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                    + "              PubTopic.TopicId,\n"
                    + "              PubTopic.ExperimentId\n"
                    + "              order by  PubTopic.ExperimentId, Project.funder, NormWeight Desc,PubTopic.ExperimentId";

            statement.executeUpdate(SQLstr);

            logger.info("Funder Trend Topic distribution");

            SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "SELECT Publication.batchId, PubTopic.TopicId, Project.funder,'FunderTrend',\n"
                    + "           round(sum(PubTopic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                    + "             PubTopic.ExperimentId\n"
                    + "      FROM PubTopic\n"
                    + "      INNER JOIN Publication on PubTopic.PubId= Publication.PubId and PubTopic.weight>0.1\n"
                    + "      and  PubTopic.ExperimentId='" + experimentId + "' \n"
                    + "      INNER JOIN  PubProject ON PubTopic.PubId = PubProject.PubId \n"
                    + "      INNER JOIN  Project ON PubProject.ProjectId = Project.ProjectId \n"
                    + "           INNER JOIN (SELECT Project.funder, Publication.batchId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                    + "           FROM PubTopic\n"
                    + "           Inner Join Publication on PubTopic.PubId= Publication.PubId and PubTopic.weight>0.1           \n"
                    + "           INNER JOIN   PubProject ON PubTopic.PubId = PubProject.PubId \n"
                    + "           INNER JOIN  Project ON PubProject.ProjectId = Project.ProjectId \n"
                    + "           GROUP BY  Project.funder,Publication.batchId,ExperimentId)\n"
                    + "           SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.funder = Project.funder AND \n"
                    + "                                           SumTopicWeightPerProjectView.ExperimentId = PubTopic.ExperimentId  AND                                          \n"
                    + "                                           SumTopicWeightPerProjectView.batchId = Publication.batchId\n"
                    + "     GROUP BY Project.funder,\n"
                    + "         Publication.batchId,\n"
                    + "              SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                    + "              PubTopic.TopicId,\n"
                    + "              PubTopic.ExperimentId\n"
                    + "              order by  PubTopic.ExperimentId, Project.funder, NormWeight Desc,PubTopic.ExperimentId";

            statement.executeUpdate(SQLstr);

            logger.info("Entity and trends topic distribution finished");

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

        logger.info("Topic similarities calculation finished");

    }

    public void CalcTopicSimilarities(String SQLLitedb) {

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            logger.info("Calc topic similarities started");

            String distinctTopicsSQL = "Select  TopicId,  ExperimentId, count(*) as cnt\n"
                    + "from TopicVector\n  "
                    + "group by TopicId,  ExperimentId";

            ResultSet rs = statement.executeQuery(distinctTopicsSQL);

            List<TopicVector> topicVectors = new ArrayList<TopicVector>();

            while (rs.next()) {

                TopicVector topicVector = new TopicVector();

                topicVector.ExperimentId = rs.getString("ExperimentId");
                topicVector.TopicId = rs.getInt("TopicId");
                //String newLabelId = experimentId + "_" + topicId;
                int dimension = rs.getInt("cnt");
                topicVector.Vector = new double[dimension];

                String selectVectorSQL = String.format("Select Weight from topicVector where ExperimentId= '%s'  and TopicId=%d order by ColumnId", topicVector.ExperimentId, topicVector.TopicId);

                Statement statement2 = connection.createStatement();
                ResultSet rs1 = statement2.executeQuery(selectVectorSQL);
                int cnt = 0;
                while (rs1.next()) {
                    topicVector.Vector[cnt++] = rs1.getDouble("Weight");
                }

                topicVectors.add(topicVector);

            }

            double similarity = 0;
            double similarityThreshold = 0.3;

            statement.executeUpdate("create table if not exists TopicSimilarity (ExperimentId1 TEXT, TopicId1 TEXT, ExperimentId2 TEXT, TopicId2 TEXT, Similarity double) ");
            String deleteSQL = String.format("Delete from TopicSimilarity");
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            String insertSql = "insert into TopicSimilarity values(?,?,?,?,?);";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(insertSql);

                for (int t1 = 0; t1 < topicVectors.size(); t1++) {
                    for (int t2 = t1; t2 < topicVectors.size(); t2++) {

                        similarity = Math.max(cosineSimilarity(topicVectors.get(t1).Vector, topicVectors.get(t2).Vector), 0);

                        if (similarity > similarityThreshold && !(topicVectors.get(t1).TopicId == topicVectors.get(t2).TopicId && topicVectors.get(t1).ExperimentId == topicVectors.get(t2).ExperimentId)) {

                            bulkInsert.setString(1, topicVectors.get(t1).ExperimentId);
                            bulkInsert.setInt(2, topicVectors.get(t1).TopicId);
                            bulkInsert.setString(3, topicVectors.get(t2).ExperimentId);
                            bulkInsert.setInt(4, topicVectors.get(t2).TopicId);
                            bulkInsert.setDouble(5, (double) Math.round(similarity * 1000) / 1000);

                            bulkInsert.executeUpdate();
                        }
                    }
                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert grantSimilarity");
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

        logger.info("Topic similarities calculation finished");

    }

    public void calcPPRSimilarities(String SQLLitedb) {
        //calc similarities

        //logger.info("PPRSimilarities calculation Started");
        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLLitedb);
            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            logger.info("PPRSimilarities calculation Started");

            // statement.executeUpdate("drop table if exists person");
//      statement.executeUpdate("create table person (id integer, name string)");
//      statement.executeUpdate("insert into person values(1, 'leo')");
//      statement.executeUpdate("insert into person values(2, 'yui')");
//      ResultSet rs = statement.executeQuery("select * from person");
            String sql = "SELECT source.OrigId||'PPR' AS PubID, target.OrigId  AS CitationId, prLinks.Counts As Counts FROM prLinks\n"
                    + "INNER JOIN PubCitationPPRAlias source ON source.RowId = PrLinks.Source\n"
                    + "INNER JOIN PubCitationPPRAlias target ON target.RowId = PrLinks.Target\n"
                    + "Union\n"
                    + "Select PubId, CitationId, 1 as Counts From PubCitation\n"
                    + "ORDER by PubId ";

            ResultSet rs = statement.executeQuery(sql);

            HashMap<String, SparseVector> labelVectors = null;
            //HashMap<String, double[]> similarityVectors = null;
            labelVectors = new HashMap<String, SparseVector>();

            String labelId = "";

            int[] citations = new int[350];
            double[] weights = new double[350];
            int cnt = 0;

            while (rs.next()) {

                String newLabelId = "";
                newLabelId = rs.getString("PubId");
                if (!newLabelId.equals(labelId) && !labelId.isEmpty()) {
                    labelVectors.put(labelId, new SparseVector(citations, weights, citations.length, citations.length, true, true, true));
                    citations = new int[350];
                    weights = new double[350];
                    cnt = 0;
                }
                labelId = newLabelId;
                citations[cnt] = rs.getInt("CitationId");
                weights[cnt] = rs.getDouble("Counts");
                cnt++;

            }

            cnt = 0;
            double similarity = 0;

            NormalizedDotProductMetric cosineSimilarity = new NormalizedDotProductMetric();

            statement.executeUpdate("create table if not exists PPRPubCitationSimilarity (PubId TEXT,  Similarity double) ");
            String deleteSQL = String.format("Delete from PPRPubCitationSimilarity");
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            sql = "insert into PPRPubCitationSimilarity values(?,?);";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (String fromPubId : labelVectors.keySet()) {

                    if (fromPubId.contains("PPR")) {
                        continue;
                    }
                    String toPubId = fromPubId + "PPR";
                    similarity = -1;

                    if (labelVectors.get(fromPubId) != null && labelVectors.get(toPubId) != null) {
                        similarity = 1 - Math.abs(cosineSimilarity.distance(labelVectors.get(fromPubId), labelVectors.get(toPubId))); // the function returns distance not similarity
                    }
                    bulkInsert.setString(1, fromPubId);
                    bulkInsert.setDouble(2, (double) Math.round(similarity * 1000) / 1000);

                    bulkInsert.executeUpdate();

                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert grantSimilarity");
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

        logger.info("Pub citation similarities calculation finished");
    }

    public void FindGroundTruthCommunities(String SQLLitedb, String experimentId) {
        //calc similarities

        //logger.info("PPRSimilarities calculation Started");
        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLLitedb);
            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            logger.info("PPRSimilarities calculation Started");

            // statement.executeUpdate("drop table if exists person");
//      statement.executeUpdate("create table person (id integer, name string)");
//      statement.executeUpdate("insert into person values(1, 'leo')");
//      statement.executeUpdate("insert into person values(2, 'yui')");
//      ResultSet rs = statement.executeQuery("select * from person");
            String sql = "select TopicId||'EXP' as GroupId, PubId as NodeId from PubTopic\n"
                    + "WHERE ExperimentId = '" + experimentId + "'\n"
                    + "UNION \n"
                    + "Select  GroupId,NodeId from GroundTruth\n"
                    + "Order By GroupId";

            ResultSet rs = statement.executeQuery(sql);

            HashMap<String, SparseVector> labelVectors = null;
            //HashMap<String, double[]> similarityVectors = null;
            labelVectors = new HashMap<String, SparseVector>();

            String labelId = "";

            int[] citations = new int[30000];
            double[] weights = new double[30000];
            int cnt = 0;

            while (rs.next()) {

                String newLabelId = "";
                newLabelId = rs.getString("GroupId");
                if (!newLabelId.equals(labelId) && !labelId.isEmpty()) {
                    labelVectors.put(labelId, new SparseVector(citations, weights, citations.length, citations.length, true, true, true));
                    citations = new int[30000];
                    weights = new double[30000];
                    cnt = 0;
                }
                labelId = newLabelId;
                citations[cnt] = rs.getInt("NodeId");
                weights[cnt] = 1; // rs.getDouble("Counts");
                cnt++;

            }

            cnt = 0;
            double similarity = 0;

            NormalizedDotProductMetric cosineSimilarity = new NormalizedDotProductMetric();

            statement.executeUpdate("create table if not exists GroundTruth2TopicId (Community TEXT,  TopicId TEXT, Similarity double, ExperimentId TEXT) ");
            String deleteSQL = String.format("Delete from GroundTruth2TopicId WHERE ExperimentId='" + experimentId + "'");
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            sql = "insert into GroundTruth2TopicId values(?,?,?,?);";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (String fromPubId : labelVectors.keySet()) {
                    //boolean startComparison = false;
                    for (String toPubId : labelVectors.keySet()) {

                        if (fromPubId.contains("EXP") || !toPubId.contains("EXP")) {
                            //if (! startComparison) {
                            //      startComparison = (fromPubId == toPubId );
                            continue;
                        }
                        //String toPubId = fromPubId + "EXP";
                        similarity = -1;

                        if (labelVectors.get(fromPubId) != null && labelVectors.get(toPubId) != null) {
                            similarity = 1 - Math.abs(cosineSimilarity.distance(labelVectors.get(fromPubId), labelVectors.get(toPubId))); // the function returns distance not similarity
                        }
                        if (similarity > 0.4) {
                            bulkInsert.setString(1, fromPubId);
                            bulkInsert.setString(2, toPubId);
                            bulkInsert.setDouble(3, (double) Math.round(similarity * 1000) / 1000);
                            bulkInsert.setString(4, experimentId);
                            bulkInsert.executeUpdate();
                        }

                    }
                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert grantSimilarity");
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

        logger.info("Pub citation similarities calculation finished");
    }

    public void calcSimilarities(String SQLLitedb, ExperimentType experimentType, String experimentId, boolean ACMAuthorSimilarity, SimilarityType similarityType, int numTopics) {
        //calc similarities

        logger.info("similarities calculation Started");
        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLLitedb);
            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            logger.info("similarities calculation Started");

            // statement.executeUpdate("drop table if exists person");
//      statement.executeUpdate("create table person (id integer, name string)");
//      statement.executeUpdate("insert into person values(1, 'leo')");
//      statement.executeUpdate("insert into person values(2, 'yui')");
//      ResultSet rs = statement.executeQuery("select * from person");
            String sql = "";
            switch (experimentType) {
//                case Grants:
//                    sql = "select    GrantId, TopicId, AVG(weight) as Weight from PubTopic Inner Join GrantPerDoc on PubTopic.PubId= GrantPerDoc.DocId"
//                            + " where weight>0.02 AND ExperimentId='" + experimentId + "' group By GrantId , TopicId order by  GrantId   , TopicId";
//                    break;
//                case FullGrants:
//                    sql = "select    project_code, TopicId, AVG(weight) as Weight from PubTopic Inner Join links  on PubTopic.PubId= links.OriginalId "
//                            + " where weight>0.02 AND ExperimentId='" + experimentId
//                            + "' group By project_code , TopicId order by  project_code, TopicId";
//
//                    break;
//                case FETGrants:
//                    sql = "select    project_code, TopicId, AVG(weight) as Weight from PubTopic Inner Join links  on PubTopic.PubId= links.OriginalId "
//                            + "   inner join projectView on links.project_code=projectView.GrantId and links.funder='FP7'  and Category1<>'NONFET'\n"
//                            + " where weight>0.02 AND ExperimentId='" + experimentId
//                            + "' group By project_code , TopicId order by  project_code, TopicId";
//
//                    break;
                case OAFullGrants:
                case Tender:
                    sql = "select EntityTopicDistribution.EntityId as projectId, EntityTopicDistribution.TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                            + "                            from EntityTopicDistribution\n"
                            + "                            where EntityTopicDistribution.EntityType='Grant' AND EntityTopicDistribution.EntityId<>'' AND\n"
                            + "                            EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                            + "                            and EntityTopicDistribution.EntityId in (Select grantId FROM PubGrant GROUP BY grantId HAVING Count(*)>4)\n"
                            + "                            and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                            + "                            where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex>1)";

                    break;
//                case Authors:
//                    sql = "select    AuthorId, TopicId, AVG(weight) as Weight from PubTopic Inner Join AuthorPerDoc on PubTopic.PubId= AuthorPerDoc.DocId"
//                            + " where weight>0.02 AND ExperimentId='" + experimentId + "' group By AuthorId , TopicId order by  AuthorId   , TopicId";
//                    break;
                case ACM:
                    if (ACMAuthorSimilarity) {
                        sql = "select EntityTopicDistribution.EntityId as authorId, EntityTopicDistribution.TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                            from EntityTopicDistribution\n"
                                + "                            where EntityTopicDistribution.EntityType='Author' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                            EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                            and EntityTopicDistribution.EntityId in (Select AuthorId FROM PubAuthor GROUP BY AuthorId HAVING Count(*)>4)\n"
                                + "                            and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                                + "                            where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex>0)";

//                        sql = "select TopicDistributionPerAuthorView.AuthorId, TopicDistributionPerAuthorView.TopicId, TopicDistributionPerAuthorView.NormWeight as Weight \n"
//                                + "from TopicDistributionPerAuthorView\n"
//                                + "where TopicDistributionPerAuthorView.experimentId='" + experimentId + "'   and TopicDistributionPerAuthorView.NormWeight>0.03\n"
//                                + "and TopicDistributionPerAuthorView.AuthorId in (Select AuthorId FROM PubAuthor GROUP BY AuthorId HAVING Count(*)>4)\n"
//                                + "and TopicDistributionPerAuthorView.topicid in (select TopicId from topicdescription \n"
//                                + "where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex>1)";
                    } else {
                        sql = "select EntityTopicDistribution.EntityId as VenueId, EntityTopicDistribution.TopicId  as TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                                                          from EntityTopicDistribution\n"
                                + "                                                          where EntityTopicDistribution.EntityType='Journal' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                                                          EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                                                          and EntityTopicDistribution.EntityId in (Select ISSN FROM PubJournal GROUP BY ISSN HAVING Count(*)>100)\n"
                                + "                                                          and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                                + "                                                          where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex=1)\n"
                                + "          UNION                                                \n"
                                + "select EntityTopicDistribution.EntityId as VenueId, EntityTopicDistribution.TopicId as TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                                                          from EntityTopicDistribution\n"
                                + "                                                          where EntityTopicDistribution.EntityType='Conference' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                                                          EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                                                          and EntityTopicDistribution.EntityId in (Select SeriesId FROM PubConference GROUP BY SeriesId HAVING Count(*)>400)\n"
                                + "                                                          and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                                + "                                                          where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex=1)";
                    }
                    //else {
//                        sql = "select    PubACMCategory.CatId as Category, TopicId, AVG(weight) as Weight from PubTopic \n"
//                                + "Inner Join PubACMCategory on PubTopic.PubId= PubACMCategory.PubId  \n"
//                                + "INNER JOIN (Select CatId FROM PubACMCategory \n"
//                                + "GROUP BY CatId HAVING Count(*)>10) catCnts1 ON catCnts1.CatId = PubACMCategory.catId\n"
//                                + "where weight>0.02 AND ExperimentId='" + experimentId + "' group By PubACMCategory.CatId , TopicId order by  PubACMCategory.CatId, Weight desc, TopicId";
//                    }

                    break;
//                case PM_pdb:
//                    sql = "select    pdbCode, TopicId, AVG(weight) as Weight from topicsPerDoc Inner Join pdblink on topicsPerDoc.DocId= pdblink.pmcId"
//                            + " where weight>0.02 AND ExperimentId='" + experimentId + "' group By pdbCode , TopicId order by  pdbCode   , TopicId";
//
//                    break;
//                case DBLP:
//                    sql = "select  Source, TopicId, AVG(weight) as Weight from PubTopic Inner Join prlinks on PubTopic.PubId= prlinks.source"
//                            + " where weight>0.02 AND ExperimentId='" + experimentId + "' group By Source , TopicId order by  Source   , TopicId";
//
//                    break;
                default:
            }

            // String sql = "select fundedarxiv.file from fundedarxiv inner join funds on file=filename Group By fundedarxiv.file LIMIT 10" ;
            ResultSet rs = statement.executeQuery(sql);

            HashMap<String, SparseVector> labelVectors = null;
            HashMap<String, double[]> similarityVectors = null;
            if (similarityType == SimilarityType.cos) {
                labelVectors = new HashMap<String, SparseVector>();
            } else {
                similarityVectors = new HashMap<String, double[]>();
            }

            String labelId = "";
            int[] topics = new int[numTopics];
            double[] weights = new double[numTopics];
            int cnt = 0;
            double a;
            while (rs.next()) {

                String newLabelId = "";

                switch (experimentType) {

                    case OAFullGrants:
                    case OAFETGrants:
                    case Tender:
                        newLabelId = rs.getString("projectId");
                        break;

                    case ACM:
                        if (ACMAuthorSimilarity) {
                            newLabelId = rs.getString("AuthorId");
                        } else {
                            newLabelId = rs.getString("VenueId");
                        }
                        break;

                    default:
                }

                if (!newLabelId.equals(labelId) && !labelId.isEmpty()) {
                    if (similarityType == SimilarityType.cos) {
                        labelVectors.put(labelId, new SparseVector(topics, weights, topics.length, topics.length, true, true, true));
                    } else {
                        similarityVectors.put(labelId, weights);
                    }
                    topics = new int[numTopics];
                    weights = new double[numTopics];
                    cnt = 0;
                }
                labelId = newLabelId;
                topics[cnt] = rs.getInt("TopicId");
                weights[cnt] = rs.getDouble("Weight");
                cnt++;

            }

            cnt = 0;
            double similarity = 0;
            double similarityThreshold = 0.15;
            NormalizedDotProductMetric cosineSimilarity = new NormalizedDotProductMetric();

            int entityType = experimentType.ordinal();
            if (experimentType == ExperimentType.ACM && !ACMAuthorSimilarity) {
                entityType = 100 + entityType;
            };

            statement.executeUpdate("create table if not exists EntitySimilarity (EntityType int, EntityId1 nvarchar(50), EntityId2 nvarchar(50), Similarity double, ExperimentId nvarchar(50)) ");
            String deleteSQL = String.format("Delete from EntitySimilarity where  ExperimentId = '%s' and entityType=%d", experimentId, entityType);
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            sql = "insert into EntitySimilarity values(?,?,?,?,?);";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                if (similarityType == SimilarityType.Jen_Sha_Div) {
                    for (String fromGrantId : similarityVectors.keySet()) {
                        boolean startCalc = false;

                        for (String toGrantId : similarityVectors.keySet()) {
                            if (!fromGrantId.equals(toGrantId) && !startCalc) {
                                continue;
                            } else {
                                startCalc = true;
                                similarity = Maths.jensenShannonDivergence(similarityVectors.get(fromGrantId), similarityVectors.get(toGrantId)); // the function returns distance not similarity
                                if (similarity > similarityThreshold && !fromGrantId.equals(toGrantId)) {

                                    bulkInsert.setInt(1, entityType);
                                    bulkInsert.setString(2, fromGrantId);
                                    bulkInsert.setString(3, toGrantId);
                                    bulkInsert.setDouble(4, (double) Math.round(similarity * 1000) / 1000);
                                    bulkInsert.setString(5, experimentId);
                                    bulkInsert.executeUpdate();
                                }
                            }
                        }
                    }
                } else if (similarityType == SimilarityType.cos) {
                    for (String fromGrantId : labelVectors.keySet()) {
                        boolean startCalc = false;

                        for (String toGrantId : labelVectors.keySet()) {
                            if (!fromGrantId.equals(toGrantId) && !startCalc) {
                                continue;
                            } else {
                                startCalc = true;
                                similarity = 1 - Math.abs(cosineSimilarity.distance(labelVectors.get(fromGrantId), labelVectors.get(toGrantId))); // the function returns distance not similarity
                                if (similarity > similarityThreshold && !fromGrantId.equals(toGrantId)) {
                                    bulkInsert.setInt(1, entityType);
                                    bulkInsert.setString(2, fromGrantId);
                                    bulkInsert.setString(3, toGrantId);
                                    bulkInsert.setDouble(4, (double) Math.round(similarity * 1000) / 1000);
                                    bulkInsert.setString(5, experimentId);
                                    bulkInsert.executeUpdate();
                                }
                            }
                        }
                    }
                }
                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in insert grantSimilarity");
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

        logger.info("similarities calculation finished");
    }

    public InstanceList[] GenerateAlphabets(String SQLLitedb, ExperimentType experimentType, String dictDir, byte numModalities,
            int pruneCnt, int pruneLblCnt, double pruneMaxPerc, double pruneMinPerc, int numChars, Net2BoWType PPRenabled, String experimentSubType, boolean ignoreText) {

        //String txtAlphabetFile = dictDir + File.separator + "dict[0].txt";
        // Begin by importing documents from text to feature sequences
        ArrayList<Pipe> pipeListText = new ArrayList<Pipe>();

        // Pipes: lowercase, tokenize, remove stopwords, map to features
        pipeListText.add(new Input2CharSequence()); //homer
        pipeListText.add(new CharSequenceLowercase());

        SimpleTokenizer tokenizer = new SimpleTokenizer(new File("stoplists/en.txt"));
        pipeListText.add(tokenizer);

        Alphabet alphabet = new Alphabet();
        pipeListText.add(new StringList2FeatureSequence(alphabet));
        pipeListText.add(new FeatureSequenceRemovePlural(alphabet));

        ArrayList<ArrayList<Instance>> instanceBuffer = new ArrayList<ArrayList<Instance>>(numModalities);
        InstanceList[] instances = new InstanceList[numModalities];

        if (!ignoreText) {
            instances[0] = new InstanceList(new SerialPipes(pipeListText));
        }
        // Other Modalities
        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {
            Alphabet alphabetM = new Alphabet();
            ArrayList<Pipe> pipeListCSV = new ArrayList<Pipe>();
            pipeListCSV.add(new CSV2FeatureSequence(alphabetM, ","));
//            if (experimentType == ExperimentType.ACM || experimentType == ExperimentType.DBLP || experimentType == ExperimentType.DBLP_ACM) {
//                pipeListCSV.add(new CSV2FeatureSequence(alphabetM, ","));
//            } else {
//                pipeListCSV.add(new CSV2FeatureSequence(alphabetM, ";"));
//            }
            instances[m] = new InstanceList(new SerialPipes(pipeListCSV));
        }

        //createCitationGraphFile("C:\\projects\\Datasets\\DBLPManage\\acm_output_NET.csv", "jdbc:sqlite:C:/projects/Datasets/DBLPManage/acm_output.db");
        for (byte m = 0; m < numModalities; m++) {
            instanceBuffer.add(new ArrayList<Instance>());

        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(SQLLitedb);
            String sql = "";

            if (experimentType == ExperimentType.ACM) {

                if (PPRenabled == Net2BoWType.PPR) {
                    sql = " select  pubId, text, fulltext, authors, citations, categories, period, keywords, venue, DBPediaResources from ACMPubView";
                } else if (PPRenabled == Net2BoWType.OneWay) {
                    sql = " select  pubId, text, fulltext, authors, citations, categories, period, keywords, venue, DBPediaResources from ACMPubViewOneWay";
                } else if (PPRenabled == Net2BoWType.TwoWay) {
                    sql = " select  pubId, text, fulltext, authors, citations, categories, period, keywords, venue, DBPediaResources from ACMPubViewTwoWay";
                }

            } else if (experimentType == ExperimentType.Tender) {

                sql = "select  pubId, text, grants, keywords, DBPediaResources from PubView";// LIMIT 100000";
//                if (experimentSubType == "PM") {
//                    sql = "select pubId, TEXT,  Funders, Venue, MESHdescriptors from HEALTHPubView_PM";
//                } else if (experimentSubType == "MT") {
//                    sql = " select pubId, TEXT,  Funders, Venue, MESHdescriptors from HEALTHPubView_MT";
//                }
            } else if (experimentType == ExperimentType.OAFullGrants) {

                sql = " select pubId, TEXT, GrantIds, Funders, Areas, AreasDescr, Venue from OpenAIREPubView";// LIMIT 100000";

            } else if (experimentType == ExperimentType.DBLP) {

                if (PPRenabled == Net2BoWType.PPR) {
                    sql = "select authorId,  Term as text, conference, coAuthor from  authorView"; // LIMIT 100";
                } else {
                    sql = "select authorId,  Term as text, conference, coAuthor from  authorViewPPR"; // LIMIT 100";
                }

            } else if (experimentType == ExperimentType.LFR) {

                if (PPRenabled == Net2BoWType.PPR) {
                    sql = "SELECT source,  \n"
                            + "GROUP_CONCAT(  distinct target|| ':' || pprLinks.Counts) AS Targets\n"
                            + "FROM pprLinks\n"
                            + "GROUP BY Source";
                } else {
                    sql = "SELECT Node1 as source,  \n"
                            + "GROUP_CONCAT(  distinct node2) AS Targets\n"
                            + "FROM Links\n"
                            + "GROUP BY node1";
                }

            } else if (experimentType == ExperimentType.DBLPNetOnly) {

                if (PPRenabled == Net2BoWType.PPR) {
                    sql = "SELECT source,  \n"
                            + "GROUP_CONCAT(  distinct target|| ':' || pprLinks.Counts) AS Targets\n"
                            + "FROM pprLinks\n"
                            + "GROUP BY Source";
                } else {
                    sql = "SELECT Author1 as source,  \n"
                            + "GROUP_CONCAT(  distinct Author2) AS Targets\n"
                            + "FROM coAuthorTwoWay \n"
                            + "GROUP BY Author1";
                }

            }

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                // read the result set
                //String lblStr = "[" + rs.getString("GrantIds") + "]" ;//+ rs.getString("text");
                //String str = "[" + rs.getString("GrantIds") + "]" + rs.getString("text");
                //System.out.println("name = " + rs.getString("file"));
                //System.out.println("name = " + rs.getString("fundings"));
                //int cnt = rs.getInt("grantsCnt");
                switch (experimentType) {

                    case ACM:
//                        instanceBuffer.get(0).add(new Instance(rs.getString("Text"), null, rs.getString("pubId"), "text"));
                        String txt = rs.getString("text");
                        instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, numChars)), null, rs.getString("pubId"), "text"));

                        if (numModalities > 1) {
                            String tmpJournalStr = rs.getString("Keywords");//.replace("\t", ",");
                            if (tmpJournalStr != null && !tmpJournalStr.equals("")) {
                                instanceBuffer.get(1).add(new Instance(tmpJournalStr.replace('-', ' ').toLowerCase(), null, rs.getString("pubId"), "Keywords"));
                            }
                        }

                        if (numModalities > 2) {
                            String tmpStr = rs.getString("Categories");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {

                                instanceBuffer.get(2).add(new Instance(tmpStr, null, rs.getString("pubId"), "category"));
                            }
                        }

                        if (numModalities > 3) {
                            String tmpAuthorsStr = rs.getString("Venue");//.replace("\t", ",");
                            if (tmpAuthorsStr != null && !tmpAuthorsStr.equals("")) {

                                instanceBuffer.get(3).add(new Instance(tmpAuthorsStr, null, rs.getString("pubId"), "Venue"));
                            }
                        }

                        if (numModalities > 4) {
                            String tmpStr = rs.getString("Citations");//.replace("\t", ",");
                            String citationStr = "";
                            if (tmpStr != null && !tmpStr.equals("")) {
                                String[] citations = tmpStr.trim().split(",");
                                for (int j = 0; j < citations.length; j++) {
                                    String[] pairs = citations[j].trim().split(":");
                                    if (pairs.length == 2) {
                                        for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                            citationStr += pairs[0] + ",";
                                        }
                                    } else {
                                        citationStr += citations[j] + ",";

                                    }
                                }
                                citationStr = citationStr.substring(0, citationStr.length() - 1);
                                instanceBuffer.get(4).add(new Instance(citationStr, null, rs.getString("pubId"), "citation"));
                            }
                        }

//DBPediaResources
                        if (numModalities > 5) {
                            String DBPediaResourcesStr = rs.getString("DBPediaResources");//.replace("\t", ",");
                            if (DBPediaResourcesStr != null && !DBPediaResourcesStr.equals("")) {

                                instanceBuffer.get(5).add(new Instance(DBPediaResourcesStr, null, rs.getString("pubId"), "DBPediaResource"));
                            }
                        }

                        if (numModalities > 6) {
                            String tmpAuthorsStr = rs.getString("Authors");//.replace("\t", ",");
                            if (tmpAuthorsStr != null && !tmpAuthorsStr.equals("")) {

                                instanceBuffer.get(6).add(new Instance(tmpAuthorsStr, null, rs.getString("pubId"), "author"));
                            }
                        }

                        if (numModalities > 7) {
                            String tmpPeriod = rs.getString("Period");//.replace("\t", ",");
                            if (tmpPeriod != null && !tmpPeriod.equals("")) {

                                instanceBuffer.get(7).add(new Instance(tmpPeriod, null, rs.getString("pubId"), "period"));
                            }
                        }

                        break;
                    case OAFullGrants:
                        //select TEXT, GrantIds, Funders, Areas, AreasDescr, Venue, MESHdescriptors
                        txt = rs.getString("text");
                        instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, numChars)), null, rs.getString("pubId"), "Text"));

                        if (numModalities > 1) {
                            String tmpStr = rs.getString("Areas");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {
                                instanceBuffer.get(1).add(new Instance(tmpStr, null, rs.getString("pubId"), "Area"));
                            }
                        }
                        ;

                        if (numModalities > 2) {
                            String tmpStr = rs.getString("Venue");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {
                                instanceBuffer.get(2).add(new Instance(tmpStr, null, rs.getString("pubId"), "Venue"));
                            }
                        }

                        if (numModalities > 3) {
                            String tmpStr = rs.getString("GrantIds");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {
                                instanceBuffer.get(3).add(new Instance(tmpStr, null, rs.getString("pubId"), "Grant"));
                            }
                        }

                        ;
                        break;

                    case Tender:
                        //select TEXT, GrantIds, Funders, Areas, AreasDescr, Venue, MESHdescriptors
                        txt = rs.getString("text");
                        instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, numChars)), null, rs.getString("pubId"), "Text"));

                        if (numModalities > 1) {
                            String tmpStr = rs.getString("DBPediaResources");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {
                                instanceBuffer.get(1).add(new Instance(tmpStr, null, rs.getString("pubId"), "DBPediaResource"));
                            }
                        }

                        if (numModalities > 2) {
                            String tmpStr = rs.getString("Grants");//.replace("\t", ",");
                            if (tmpStr != null && !tmpStr.equals("")) {
                                instanceBuffer.get(2).add(new Instance(tmpStr, null, rs.getString("pubId"), "Grant"));
                            }
                        }

                        ;
                        break;

                    case LFR:

                        String tmpStr = rs.getString("Targets");//.replace("\t", ",");
                        String targetStr = "";
                        if (tmpStr != null && !tmpStr.equals("")) {
                            String[] citations = tmpStr.trim().split(",");
                            for (int j = 0; j < citations.length; j++) {
                                String[] pairs = citations[j].trim().split(":");
                                if (pairs.length == 2) {
                                    for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                        targetStr += pairs[0] + ",";
                                    }
                                } else {
                                    targetStr += citations[j] + ",";

                                }
                            }
                            targetStr = targetStr.substring(0, targetStr.length() - 1);
                            instanceBuffer.get(0).add(new Instance(targetStr, null, rs.getString("source"), "link"));
                        }

                        break;
                    case DBLPNetOnly:

                        String tmpCoAuthorStr = rs.getString("Targets");//.replace("\t", ",");
                        String targetAuthorStr = "";
                        if (tmpCoAuthorStr != null && !tmpCoAuthorStr.equals("")) {
                            String[] citations = tmpCoAuthorStr.trim().split(",");
                            for (int j = 0; j < citations.length; j++) {
                                String[] pairs = citations[j].trim().split(":");
                                if (pairs.length == 2) {
                                    for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                        targetAuthorStr += pairs[0] + ",";
                                    }
                                } else {
                                    targetAuthorStr += citations[j] + ",";

                                }
                            }
                            targetStr = targetAuthorStr.substring(0, targetAuthorStr.length() - 1);
                            instanceBuffer.get(0).add(new Instance(targetAuthorStr, null, rs.getString("source"), "link"));
                        }

                        break;
                    case DBLP:

                        String tmpFieldValue = rs.getString("text");//.replace("\t", ",");
                        String newValueStr = "";
                        if (tmpFieldValue != null && !tmpFieldValue.equals("")) {
                            String[] citations = tmpFieldValue.trim().split(",");
                            for (int j = 0; j < citations.length; j++) {
                                String[] pairs = citations[j].trim().split(":");
                                if (pairs.length == 2) {
                                    for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                        newValueStr += pairs[0] + ",";
                                    }
                                } else {
                                    newValueStr += citations[j] + ",";

                                }
                            }
                            targetStr = newValueStr.substring(0, newValueStr.length() - 1);
                            instanceBuffer.get(0).add(new Instance(newValueStr, null, rs.getString("authorId"), "text"));
                        }

                        if (numModalities > 1) {
                            tmpFieldValue = rs.getString("conference");//.replace("\t", ",");
                            newValueStr = "";
                            if (tmpFieldValue != null && !tmpFieldValue.equals("")) {
                                String[] citations = tmpFieldValue.trim().split(",");
                                for (int j = 0; j < citations.length; j++) {
                                    String[] pairs = citations[j].trim().split(":");
                                    if (pairs.length == 2) {
                                        for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                            newValueStr += pairs[0] + ",";
                                        }
                                    } else {
                                        newValueStr += citations[j] + ",";

                                    }
                                }
                                targetStr = newValueStr.substring(0, newValueStr.length() - 1);
                                instanceBuffer.get(1).add(new Instance(newValueStr, null, rs.getString("authorId"), "conference"));
                            }
                        }

                        if (numModalities > 2) {
                            tmpFieldValue = rs.getString("coAuthor");//.replace("\t", ",");
                            newValueStr = "";
                            if (tmpFieldValue != null && !tmpFieldValue.equals("")) {
                                String[] citations = tmpFieldValue.trim().split(",");
                                for (int j = 0; j < citations.length; j++) {
                                    String[] pairs = citations[j].trim().split(":");
                                    if (pairs.length == 2) {
                                        for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                            newValueStr += pairs[0] + ",";
                                        }
                                    } else {
                                        newValueStr += citations[j] + ",";

                                    }
                                }
                                targetStr = newValueStr.substring(0, newValueStr.length() - 1);
                                instanceBuffer.get(2).add(new Instance(newValueStr, null, rs.getString("authorId"), "coAuthor"));
                            }
                        }

                        break;
                    default:
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

        logger.info("Read " + instanceBuffer.get(0).size() + " instances modality: " + instanceBuffer.get(0).get(0).getSource().toString());

        if (!ignoreText) {
            try {
                GenerateStoplist(tokenizer, instanceBuffer.get(0), pruneCnt, pruneMaxPerc, pruneMinPerc, false);
                instances[0].addThruPipe(instanceBuffer.get(0).iterator());
                //Alphabet tmpAlp = instances[0].getDataAlphabet();
                //ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(txtAlphabetFile)));
                //oos.writeObject(tmpAlp);
                //oos.close();
            } catch (IOException e) {
                System.err.println("Problem adding text: "
                        + e);
            }
        }

        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {
            logger.info("Read " + instanceBuffer.get(m).size() + " instances modality: " + (instanceBuffer.get(m).size() > 0 ? instanceBuffer.get(m).get(0).getSource().toString() : m));
            //instances[m] = new InstanceList(new SerialPipes(pipeListCSV));
            instances[m].addThruPipe(instanceBuffer.get(m).iterator());
        }

        logger.info(" instances added through pipe");

        // pruning for all other modalities no text
        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {
            if ((m == 0 && pruneCnt > 0) || (m > 0 && pruneLblCnt > 0)) {

                // Check which type of data element the instances contain
                Instance firstInstance = instances[m].get(0);
                if (firstInstance.getData() instanceof FeatureSequence) {
                    // Version for feature sequences

                    Alphabet oldAlphabet = instances[m].getDataAlphabet();
                    Alphabet newAlphabet = new Alphabet();

                    // It's necessary to create a new instance list in
                    //  order to make sure that the data alphabet is correct.
                    Noop newPipe = new Noop(newAlphabet, instances[m].getTargetAlphabet());
                    InstanceList newInstanceList = new InstanceList(newPipe);

                    // Iterate over the instances in the old list, adding
                    //  up occurrences of features.
                    int numFeatures = oldAlphabet.size();
                    double[] counts = new double[numFeatures];
                    for (int ii = 0; ii < instances[m].size(); ii++) {
                        Instance instance = instances[m].get(ii);
                        FeatureSequence fs = (FeatureSequence) instance.getData();

                        fs.addFeatureWeightsTo(counts);
                    }

                    Instance instance;

                    // Next, iterate over the same list again, adding 
                    //  each instance to the new list after pruning.
                    while (instances[m].size() > 0) {
                        instance = instances[m].get(0);
                        FeatureSequence fs = (FeatureSequence) instance.getData();

                        fs.prune(counts, newAlphabet, ((m == 4 && experimentType == ExperimentType.ACM && PPRenabled == Net2BoWType.PPR)) ? pruneLblCnt * 3 : pruneLblCnt);

                        newInstanceList.add(newPipe.instanceFrom(new Instance(fs, instance.getTarget(),
                                instance.getName(),
                                instance.getSource())));

                        instances[m].remove(0);
                    }

//                logger.info("features: " + oldAlphabet.size()
                    //                       + " -> " + newAlphabet.size());
                    // Make the new list the official list.
                    instances[m] = newInstanceList;
                    // Alphabet tmp = newInstanceList.getDataAlphabet();
//                    String modAlphabetFile = dictDir + File.separator + "dict[" + m + "].txt";
//                    try {
//                        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(modAlphabetFile)));
//                        oos.writeObject(tmp);
//                        oos.close();
//                    } catch (IOException e) {
//                        System.err.println("Problem serializing modality " + m + " alphabet to file "
//                                + txtAlphabetFile + ": " + e);
//                    }

                } else {
                    throw new UnsupportedOperationException("Pruning features from "
                            + firstInstance.getClass().getName()
                            + " is not currently supported");
                }

            }
        }
        return instances;

    }

    public void createRefACMTables(String SQLLitedb) {
        //String SQLLitedb = "jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLLitedb);

            Statement statement = connection.createStatement();
            statement.executeUpdate("create table if not exists Author (AuthorId nvarchar(50), FirstName nvarchar(50), LastName nvarchar(50), MiddleName nvarchar(10), Affilication TEXT) ");
            String deleteSQL = String.format("Delete from Author ");
            statement.executeUpdate(deleteSQL);

            statement.executeUpdate("create table if not exists Citation (CitationId nvarchar(50), Reference TEXT) ");
            deleteSQL = String.format("Delete from Citation ");
            statement.executeUpdate(deleteSQL);

            statement.executeUpdate("create table if not exists Category (Category TEXT) ");
            deleteSQL = String.format("Delete from Category ");
            statement.executeUpdate(deleteSQL);

            statement = connection.createStatement();
            statement.executeUpdate("create table if not exists PubAuthor (PubId nvarchar(50), AuthorId nvarchar(50)) ");
            deleteSQL = String.format("Delete from PubAuthor ");
            statement.executeUpdate(deleteSQL);

            statement.executeUpdate("create table if not exists PubCitation (PubId nvarchar(50), CitationId nvarchar(50)) ");
            deleteSQL = String.format("Delete from PubCitation ");
            statement.executeUpdate(deleteSQL);

            statement.executeUpdate("create table if not exists PubCategory (PubId nvarchar(50), Category TEXT) ");
            deleteSQL = String.format("Delete from PubCategory ");
            statement.executeUpdate(deleteSQL);

            PreparedStatement authorBulkInsert = null;
            PreparedStatement citationBulkInsert = null;
            PreparedStatement categoryBulkInsert = null;

            PreparedStatement pubAuthorBulkInsert = null;
            PreparedStatement pubCitationBulkInsert = null;
            PreparedStatement pubCategoryBulkInsert = null;

            String authorInsertsql = "insert into Author values(?,?,?,?,?);";
            String citationInsertsql = "insert into Citation values(?,?);";
            String categoryInsertsql = "insert into Category values(?);";

            String pubAuthorInsertsql = "insert into pubAuthor values(?,?);";
            String pubCitationInsertsql = "insert into pubCitation values(?,?);";
            String pubCategoryInsertsql = "insert into pubCategory values(?,?);";

            TObjectIntHashMap<String> authorsLst = new TObjectIntHashMap<String>();
            TObjectIntHashMap<String> citationsLst = new TObjectIntHashMap<String>();
            TObjectIntHashMap<String> categorysLst = new TObjectIntHashMap<String>();

            try {

                connection.setAutoCommit(false);
                authorBulkInsert = connection.prepareStatement(authorInsertsql);
                citationBulkInsert = connection.prepareStatement(citationInsertsql);
                categoryBulkInsert = connection.prepareStatement(categoryInsertsql);

                pubAuthorBulkInsert = connection.prepareStatement(pubAuthorInsertsql);
                pubCitationBulkInsert = connection.prepareStatement(pubCitationInsertsql);
                pubCategoryBulkInsert = connection.prepareStatement(pubCategoryInsertsql);

                String sql = "	Select articleid,authors_id,authors_firstname,authors_lastname,authors_middlename,authors_affiliation,authors_role, \n"
                        + "			ref_objid,reftext,primarycategory,othercategory \n"
                        + "			 from ACMData1 \n";
                // + "			  LIMIT 10";

                ResultSet rs = statement.executeQuery(sql);

                while (rs.next()) {
                    // read the result set
                    String Id = rs.getString("articleid");

                    String authorIdsStr = rs.getString("authors_id");
                    String[] authorIds = authorIdsStr.split("\t");

                    String authors_firstnamesStr = rs.getString("authors_firstname");
                    String[] authors_firstnames = authors_firstnamesStr.split("\t");

                    String authors_lastnamesStr = rs.getString("authors_lastname");
                    String[] authors_lastnames = authors_lastnamesStr.split("\t");

                    String authors_middlenamesStr = rs.getString("authors_middlename");
                    String[] authors_middlenames = authors_middlenamesStr.split("\t");

                    String authors_affiliationsStr = rs.getString("authors_affiliation");
                    String[] authors_affiliations = authors_affiliationsStr.split("\t");

                    for (int i = 0; i < authorIds.length - 1; i++) {
                        String authorId = authorIds[i];
                        if (!authorsLst.containsKey(authorId)) {
                            authorsLst.put(authorId, 1);
                            String lstName = authors_lastnames.length - 1 > i ? authors_lastnames[i] : "";
                            String fstName = authors_firstnames.length - 1 > i ? authors_firstnames[i] : "";
                            String mName = authors_middlenames.length - 1 > i ? authors_middlenames[i] : "";
                            String affiliation = authors_affiliations.length - 1 > i ? authors_affiliations[i] : "";

                            authorBulkInsert.setString(1, authorId);
                            authorBulkInsert.setString(2, lstName);
                            authorBulkInsert.setString(3, fstName);
                            authorBulkInsert.setString(4, mName);
                            authorBulkInsert.setString(5, affiliation);
                            authorBulkInsert.executeUpdate();
                        }
                        pubAuthorBulkInsert.setString(1, Id);
                        pubAuthorBulkInsert.setString(2, authorId);
                        pubAuthorBulkInsert.executeUpdate();

                    }

                    String citationIdsStr = rs.getString("ref_objid");
                    String[] citationIds = citationIdsStr.split("\t");

                    String citationsStr = rs.getString("reftext");
                    String[] citations = citationsStr.split("\t");

                    for (int i = 0; i < citationIds.length - 1; i++) {
                        String citationId = citationIds[i];
                        if (!citationsLst.containsKey(citationId)) {
                            citationsLst.put(citationId, 1);
                            String ref = citations.length - 1 > i ? citations[i] : "";

                            citationBulkInsert.setString(1, citationId);
                            citationBulkInsert.setString(2, ref);
                            citationBulkInsert.executeUpdate();
                        }
                        pubCitationBulkInsert.setString(1, Id);
                        pubCitationBulkInsert.setString(2, citationId);
                        pubCitationBulkInsert.executeUpdate();
                    }

                    String prCategoriesStr = rs.getString("primarycategory");
                    String[] prCategories = prCategoriesStr.split("\t");

                    String categoriesStr = rs.getString("othercategory");
                    String[] categories = categoriesStr.split("\t");

                    for (int i = 0; i < prCategories.length - 1; i++) {
                        String category = prCategories[i];
                        if (!categorysLst.containsKey(category)) {
                            categorysLst.put(category, 1);
                            categoryBulkInsert.setString(1, category);
                            categoryBulkInsert.executeUpdate();
                        }
                        pubCategoryBulkInsert.setString(1, Id);
                        pubCategoryBulkInsert.setString(2, category);
                        pubCategoryBulkInsert.executeUpdate();
                    }

                    for (int i = 0; i < categories.length - 1; i++) {
                        String category = categories[i];
                        if (!categorysLst.containsKey(category)) {
                            categorysLst.put(category, 1);
                            categoryBulkInsert.setString(1, category);
                            categoryBulkInsert.executeUpdate();
                        }

                        pubCategoryBulkInsert.setString(1, Id);
                        pubCategoryBulkInsert.setString(2, category);
                        pubCategoryBulkInsert.executeUpdate();
                    }

                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        System.err.print("Error in ACMReferences extraction");
                    }
                }
            } finally {

                if (authorBulkInsert != null) {
                    authorBulkInsert.close();
                }
                if (categoryBulkInsert != null) {
                    categoryBulkInsert.close();
                }
                if (citationBulkInsert != null) {
                    citationBulkInsert.close();
                }
                connection.setAutoCommit(true);
            }

        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.println("File input error");
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

    public static void main(String[] args) throws Exception {
        Class.forName("org.sqlite.JDBC");
        PTMExperiment trainer = new PTMExperiment();

    }
}

/*
 if (!new File(txtAlphabetFile).exists()) {

 GenerateAlphabets(SQLLitedb, experimentType, dictDir, numModalities, pruneCntPerc, pruneLblCntPerc, pruneMaxPerc, pruneMinPerc);

 }
 //read alphabets

 for (byte m = 0; m < numModalities; m++) {
 String modAlphabetFile = dictDir + File.separator + "dict[" + m + "].txt";
 if (new File(modAlphabetFile).exists()) {

 try {
 ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(modAlphabetFile)));
 alphabets[m] = (Alphabet) ois.readObject();
 ois.close();
 } catch (Exception e) {
 // if the error message is "out of memory", 
 // it probably means no database file is found
 System.err.println(e.getMessage());
 alphabets[m] = new Alphabet();
 }
 } else {
 alphabets[m] = new Alphabet();
 }
 }

 ArrayList<String> batchIds = new ArrayList<String>();
 // Select BatchIds
 try {

 connection = DriverManager.getConnection(SQLLitedb);
 String sql = "";

 if (runOnLine) {
 sql = "select distinct batchId from Publication";
 Statement statement = connection.createStatement();
 ResultSet rs = statement.executeQuery(sql);
 while (rs.next()) {
 batchIds.add(rs.getString("batchId"));
 }

 } else {
 batchIds.add("-1"); // noBatches
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

 //
 //            if (calcTokensPerEntity) {
 //                TfIdfWeighting(instances[0], SQLLitedb, experimentId, 1);
 //            }
 //createCitationGraphFile("C:\\projects\\Datasets\\DBLPManage\\acm_output_NET.csv", "jdbc:sqlite:C:/projects/Datasets/DBLPManage/acm_output.db");
 ArrayList<ArrayList<Instance>> instanceBuffer = new ArrayList<ArrayList<Instance>>(numModalities);
 for (byte m = 0; m < numModalities; m++) {
 instanceBuffer.add(new ArrayList<Instance>());

 }
 */
// Loop for every batch
// for (String batchId : batchIds) {
/*             try {

 // clear previous lists
 for (byte m = 0; m < numModalities; m++) {
 instanceBuffer.get(m).clear();
 }

 connection = DriverManager.getConnection(SQLLitedb);
 String sql = "";

 if (experimentType == ExperimentType.ACM) {
 experimentDescription = "Topic modeling based on:\n1)Full text from ACM publications \n2)Authors\n3)Citations\n4)ACMCategories\n SimilarityType:"
 + similarityType.toString()
 + "\n Similarity on Authors & Categories";
 //+ (ACMAuthorSimilarity ? "Authors" : "Categories");

 sql = batchId == "-1"
 ? " select  pubId, fulltext, authors, citations, categories, period from ACMPubView"
 : "select  pubId, fulltext, authors, citations, categories, period from ACMPubView where batchId = '" + batchId + "'";

 sql += " LIMIT 10000";

 }

 // String sql = "select fundedarxiv.file from fundedarxiv inner join funds on file=filename Group By fundedarxiv.file LIMIT 10" ;
 Statement statement = connection.createStatement();
 ResultSet rs = statement.executeQuery(sql);
 String txt = "";
 while (rs.next()) {
 // read the result set
 //String lblStr = "[" + rs.getString("GrantIds") + "]" ;//+ rs.getString("text");
 //String str = "[" + rs.getString("GrantIds") + "]" + rs.getString("text");
 //System.out.println("name = " + rs.getString("file"));
 //System.out.println("name = " + rs.getString("fundings"));
 //int cnt = rs.getInt("grantsCnt");
 switch (experimentType) {

 case ACM:
 txt = rs.getString("fulltext");
 instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, 10000)), null, rs.getString("pubId"), "text"));

 //instanceBuffer.get(0).add(new Instance(rs.getString("Text"), null, rs.getString("pubId"), "text"));
 if (numModalities > 1) {
 String tmpStr = rs.getString("Citations");//.replace("\t", ",");
 instanceBuffer.get(1).add(new Instance(tmpStr, null, rs.getString("pubId"), "citation"));
 }
 if (numModalities > 2) {
 String tmpStr = rs.getString("Categories");//.replace("\t", ",");
 instanceBuffer.get(2).add(new Instance(tmpStr, null, rs.getString("pubId"), "category"));
 }
 if (numModalities > 3) {
 String tmpAuthorsStr = rs.getString("Authors");//.replace("\t", ",");
 instanceBuffer.get(3).add(new Instance(tmpAuthorsStr, null, rs.getString("pubId"), "author"));
 }

 if (numModalities > 4) {
 String tmpPeriodStr = rs.getString("Period");//.replace("\t", ",");
 instanceBuffer.get(4).add(new Instance(tmpPeriodStr, null, rs.getString("pubId"), "period"));
 }
 break;

 default:
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

 logger.info("Read " + instanceBuffer.get(0).size() + " instances modality: " + instanceBuffer.get(0).get(0).getSource().toString());

 // Begin by importing documents from text to feature sequences
 ArrayList<Pipe> pipeListText = new ArrayList<Pipe>();

 // Pipes: lowercase, tokenize, remove stopwords, map to features
 pipeListText.add(new Input2CharSequence(false)); //homer
 pipeListText.add(new CharSequenceLowercase());

 SimpleTokenizer tokenizer = new SimpleTokenizer(0); // empty stop list (new File("stoplists/en.txt"));
 pipeListText.add(tokenizer);

 pipeListText.add(new StringList2FeatureSequence(alphabets[0]));

 InstanceList[] instances = new InstanceList[numModalities];
 instances[0] = new InstanceList(new SerialPipes(pipeListText));

 // Other Modalities
 for (byte m = 1; m < numModalities; m++) {

 ArrayList<Pipe> pipeListCSV = new ArrayList<Pipe>();
 if (experimentType == ExperimentType.ACM || experimentType == ExperimentType.DBLP || experimentType == ExperimentType.DBLP_ACM) {
 pipeListCSV.add(new CSV2FeatureSequence(alphabets[m], ","));
 } else {
 pipeListCSV.add(new CSV2FeatureSequence(alphabets[m], ";"));
 }
 instances[m] = new InstanceList(new SerialPipes(pipeListCSV));
 }

 instances[0].addThruPipe(instanceBuffer.get(0).iterator());

 for (byte m = 1; m < numModalities; m++) {
 logger.info("Read " + instanceBuffer.get(m).size() + " instances modality: " + (instanceBuffer.get(m).size() > 0 ? instanceBuffer.get(m).get(0).getSource().toString() : m));
 //instances[m].clear();
 instances[m].addThruPipe(instanceBuffer.get(m).iterator());
 }

 //
 //Alphabet[] existedAlphabets = new Alphabet[numModalities];
 //                for (byte m = 1; m < numModalities; m++) {
 //                    logger.info("Read " + instanceBuffer.get(m).size() + " instances modality: " + (instanceBuffer.get(m).size() > 0 ? instanceBuffer.get(m).get(0).getSource().toString() : m));
 //                    instances[m].clear(); // = new InstanceList(new SerialPipes(pipeListCSV));
 //                    //existedAlphabets[m] = instances[m].getDataAlphabet();
 //                    instances[m].addThruPipe(instanceBuffer.get(m).iterator());
 //                }
           
 String batchId = "-1";
 InstanceList[] instances = GenerateAlphabets(SQLLitedb, experimentType, dictDir, numModalities, pruneCntPerc, pruneLblCntPerc, pruneMaxPerc, pruneMinPerc, numChars);
 logger.info(" instances added through pipe");
 */
