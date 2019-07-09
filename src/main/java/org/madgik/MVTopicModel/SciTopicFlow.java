package org.madgik.MVTopicModel;

import cc.mallet.pipe.*;
import cc.mallet.types.*;
import cc.mallet.util.Maths;
import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.madgik.utils.CSV2FeatureSequence;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.madgik.utils.Utils.cosineSimilarity;

public class SciTopicFlow {

    public enum ExperimentType {

        ACM,
        PubMed

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

    public static Logger logger = Logger.getLogger("SciTopic");

    private int topWords = 20;
    private int showTopicsInterval = 50;
    private byte numModalities = 6;

    private int numOfThreads = 4;
    private int numTopics = 400;
    private int numIterations = 200; //Max 2000
    private int numChars = 4000;
    private int burnIn = 50;
    private int optimizeInterval = 50;
    private  ExperimentType experimentType = ExperimentType.ACM;

    private double pruneCntPerc = 0.002;    //Remove features that appear less than PruneCntPerc* TotalNumberOfDocuments times (-->very rare features)
    private double pruneLblCntPerc = 0.002;   //Remove features that appear less than PruneCntPerc* TotalNumberOfDocuments times (-->very rare features)
    private double pruneMaxPerc = 10;//Remove features that occur in more than (X)% of documents. 0.05 is equivalent to IDF of 3.0.

    private boolean ACMAuthorSimilarity = true;
    private boolean calcTopicDistributionsAndTrends = false;
    private boolean calcEntitySimilarities = false;
    private boolean calcTopicSimilarities = false;
    private boolean calcPPRSimilarities = false;
    private boolean runTopicModelling = true;
    private boolean runInference = true;
    private boolean runWordEmbeddings = false;
    private boolean useTypeVectors = false;
    private boolean trainTypeVectors = false;
    private boolean findKeyPhrases = false;

    private double useTypeVectorsProb = 0.2;
    private Net2BoWType PPRenabled = Net2BoWType.OneWay;
    private int vectorSize = 200;
    private String SQLConnectionString = "jdbc:postgresql://localhost:5432/tender?user=postgres&password=postgres&ssl=false"; //"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
    private String experimentId = "";
    private String previousModelFile = "";
    private int limitDocs = 10000;
    private boolean D4I = true;

    public SciTopicFlow() throws IOException {
        this(null);
    }

    public SciTopicFlow(Map<String, String> runtimeProp) throws IOException {

        SimilarityType similarityType = SimilarityType.cos; //Cosine 1 jensenShannonDivergence 2 symmetric KLP

        String dictDir = "";

        Connection connection = null;

        getPropValues(runtimeProp);

        String experimentString = experimentType.toString() + "_" + numTopics + "T_"
                + numIterations + "IT_" + numChars + "CHRs_" + numModalities + "M_" + (trainTypeVectors ? "WV" : "") + ((limitDocs > 0) ? ("Lmt_" + limitDocs) : "") + PPRenabled.name();

        String experimentDetails = String.format("Multi View Topic Modeling Analysis \n pruneMaxPerc:%.1f  pruneCntPerc:%.4f pruneLblCntPerc:%.4f burnIn:%d numOfThreads:%d similarityType:%s", this.pruneMaxPerc, pruneCntPerc, pruneLblCntPerc, burnIn, numOfThreads, similarityType.toString());

        String experimentDescription = experimentString + ": \n";

        if (runtimeProp != null) {
            experimentId = runtimeProp.get("ExperimentId");
        }

        if (StringUtils.isBlank(experimentId)) {
            experimentId = experimentString;
        }

        if (findKeyPhrases) {
            FindKeyPhrasesPerTopic(SQLConnectionString, experimentId, "openNLP");

        }

        if (runWordEmbeddings) {
            logger.info(" calc word embeddings starting");
            InstanceList[] instances = ImportInstancesWithNewPipes(ReadDataFromDB(SQLConnectionString, experimentType, numModalities, limitDocs, ""), experimentType, numModalities,
                    pruneCntPerc, pruneLblCntPerc, pruneMaxPerc, false, (experimentType == ExperimentType.PubMed) ? ";" : ",");

            logger.info(" instances added through pipe");

            //int numDimensions = 50;
            int windowSizeOption = 5;
            int numSamples = 5;
            int numEpochs = 5;
            WordEmbeddings matrix = new WordEmbeddings(instances[0].getDataAlphabet(), vectorSize, windowSizeOption);
            //TopicWordEmbeddings matrix = new TopicWordEmbeddings(instances[0].getDataAlphabet(), vectorSize, windowSizeOption,0);
            matrix.queryWord = "skin";
            matrix.countWords(instances[0], 0.0001); //Sampling factor : "Down-sample words that account for more than ~2.5x this proportion or the corpus."
            matrix.train(instances[0], numOfThreads, numSamples, numEpochs);
            logger.info(" calc word embeddings ended");
            //PrintWriter out = new PrintWriter("vectors.txt");
            //matrix.write(out);
            //out.close();
            matrix.write(SQLConnectionString, 0);
            logger.info(" writing word embeddings ended");
        }

        if (runTopicModelling) {

            logger.info(" TopicModelling has started");
            String batchId = "-1";

            InstanceList[] instances = ImportInstancesWithNewPipes(ReadDataFromDB(SQLConnectionString, experimentType, numModalities, limitDocs, ""), experimentType, numModalities,
                    pruneCntPerc, pruneLblCntPerc, pruneMaxPerc, false, (experimentType == ExperimentType.PubMed) ? ";" : ",");

            logger.info("Instances added through pipe");

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
            FastQMVWVParallelTopicModel model = new FastQMVWVParallelTopicModel(numTopics, numModalities, alpha, beta, useCycleProposals, SQLConnectionString, useTypeVectors, useTypeVectorsProb, trainTypeVectors);
            model.setSaveSerializedModel(50, "experimentId.model");

            model.DeletePreviousExperiment(SQLConnectionString, experimentId);

            // ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);
            model.setNumIterations(numIterations);
            model.setTopicDisplay(showTopicsInterval, topWords);
            // model.setIndependentIterations(independentIterations);
            model.optimizeInterval = optimizeInterval;
            model.burninPeriod = burnIn;
            model.setNumThreads(numOfThreads);

            model.addInstances(instances, batchId, vectorSize, null);//trainingInstances);//instances);
            logger.info(" instances added");

            //model.readWordVectorsDB(SQLConnectionString, vectorSize);
            model.estimate();
            logger.info("Model estimated");

            model.saveResults(SQLConnectionString, experimentId, experimentDetails);
            logger.info("Model saved");

            logger.info("Model Id: \n" + experimentId);
            logger.info("Model Metadata: \n" + model.getExpMetadata());

            //if (modelEvaluationFile != null) {
            try {

                double perplexity = 0;

                FastQMVWVTopicModelDiagnostics diagnostics = new FastQMVWVTopicModelDiagnostics(model, topWords);
                diagnostics.saveToDB(SQLConnectionString, experimentId, perplexity, batchId);
                logger.info("full diagnostics calculation finished");

            } catch (Exception e) {

                logger.error(e.getMessage());
            }

        }

        if (runInference) {

            logger.info(" Inference on new docs has started");
            String batchId = "-1";

            FastQMVWVTopicInferencer inferencer = null;
            /*
            String inferencerFilename = "";

            if (!inferencerFilename.isEmpty()) {

                try {
                    inferencer = FastQMVWVTopicInferencer.read(new File(inferencerFilename));
                } catch (Exception e) {
                    logger.error("Unable to restore saved topic model "
                            + inferencerFilename + ": " + e);

                }
            }
            */
            
          

                try {
                    inferencer = FastQMVWVTopicInferencer.read(SQLConnectionString, experimentId);
                } catch (Exception e) {
                    logger.error("Unable to restore saved topic model "
                            + experimentId + ": " + e);

                }
          

                InstanceList[] instances = ImportInstancesWithExistingPipes(
                        ReadDataFromDB(SQLConnectionString, experimentType, numModalities, 10, "WHERE batchid>'2018'"), 
                        inferencer.getPipes(), numModalities);

                logger.info("Instances added through pipe");

                inferencer.inferTopicDistributionsOnNewDocs(instances, SQLConnectionString, experimentId, null);
           
            }

            if (calcTopicDistributionsAndTrends) {

                CalcEntityTopicDistributionsAndTrends(SQLConnectionString, experimentId);

            }

            if (calcEntitySimilarities) {

                calcSimilarities(SQLConnectionString, experimentType, experimentId, ACMAuthorSimilarity, similarityType, numTopics);

            }

            if (calcTopicSimilarities) {
                experimentId = "HEALTHTenderPM_500T_600IT_7000CHRs_10.0 3.0E-4_2.0E-4PRN50B_4M_4TH_cosOneWay";
                CalcTopicSimilarities(SQLConnectionString, experimentId);
            }

            if (calcPPRSimilarities) {
                calcPPRSimilarities(SQLConnectionString);
            }

        }

    

    public void getPropValues(Map<String, String> runtimeProp) throws IOException {

        InputStream inputStream = null;
        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            if (runtimeProp != null) {
                prop.putAll(runtimeProp);
            }

            // get the property value and print it out
            numTopics = Integer.parseInt(prop.getProperty("TopicsNumber"));
            topWords = Integer.parseInt(prop.getProperty("TopWords"));
            numModalities = Byte.parseByte(prop.getProperty("NumModalities"));
            numIterations = Integer.parseInt(prop.getProperty("Iterations"));
            numOfThreads = Integer.parseInt(prop.getProperty("NumOfThreads"));
            numChars = Integer.parseInt(prop.getProperty("NumOfChars"));
            burnIn = Integer.parseInt(prop.getProperty("BurnIn"));
            optimizeInterval = Integer.parseInt(prop.getProperty("OptimizeInterval"));
            pruneCntPerc = Double.parseDouble(prop.getProperty("PruneCntPerc"));
            pruneLblCntPerc = Double.parseDouble(prop.getProperty("PruneLblCntPerc"));
            pruneMaxPerc = Double.parseDouble(prop.getProperty("PruneMaxPerc"));
            SQLConnectionString = prop.getProperty("SQLConnectionString");
            experimentId = prop.getProperty("ExperimentId");

        } catch (Exception e) {
            logger.error("Exception in reading properties: " + e);
        } finally {
            inputStream.close();
        }

    }

//    private void writeProperties() {
//        Properties prop = new Properties();
//        OutputStream output = null;
//
//        try {
//
//            String propFileName = "config.properties";
//
//            output = getClass().getClassLoader().getResourceAsStream(propFileName);
//
//            if (inputStream != null) {
//                prop.load(inputStream);
//            } else {
//                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
//            }
//            
//            output = new FileOutputStream("config.properties");
//            
//            
//            // set the properties value
//            prop.setProperty("topWords", String.valueOf(topWords));
//            prop.setProperty("SQLConnectionString", SQLConnectionString);
//            prop.setProperty("showTopicsInterval", String.valueOf(showTopicsInterval));
//            prop.setProperty("numModalities", numModalities);
//            prop.setProperty("numOfThreads", numOfThreads);
//            prop.setProperty("numTopics", numTopics);
//            prop.setProperty("numIterations", numIterations);
//            prop.setProperty("numChars", numChars);
//            prop.setProperty("burnIn", burnIn);
//            prop.setProperty("optimizeInterval", optimizeInterval);
//            prop.setProperty("experimentType", experimentType);
//            prop.setProperty("pruneCntPerc", pruneCntPerc);
//            prop.setProperty("pruneLblCntPerc", pruneLblCntPerc);
//            prop.setProperty("pruneMaxPerc", pruneMaxPerc);
//            prop.setProperty("pruneMinPerc", pruneMinPerc);
//            prop.setProperty("calcEntitySimilarities", calcEntitySimilarities);
//            prop.setProperty("runTopicModelling", runTopicModelling);
//            prop.setProperty("findKeyPhrases", findKeyPhrases);
//            prop.setProperty("PPRenabled", PPRenabled);
//           
//
//            // save properties to project root folder
//            prop.store(output, null);
//
//        } catch (IOException io) {
//            io.printStackTrace();
//        } finally {
//            if (output != null) {
//                try {
//                    output.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//        }
//    }
    private void FindKeyPhrasesPerTopic(String SQLConnection, String experimentId, String tagger) {
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
            //connection = DriverManager.getConnection(SQLConnectionString);
            connection = DriverManager.getConnection(SQLConnection);
            Statement statement = connection.createStatement();

            logger.info("Finding key phrases calculation started");

            String sql = "select doc_topic.TopicId, document.title, document.abstract from \n"
                    + "doc_topic\n"
                    + "inner join document on doc_topic.docId= document.docid and doc_topic.Weight>0.55 \n"
                    + "where experimentId='" + experimentId + "' \n"
                    + "order by doc_topic.topicid, weight desc";

            ResultSet rs = statement.executeQuery(sql);

            HashMap<Integer, Map<String, ArrayList<Integer>>> topicTitles;

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

            statement.executeUpdate("create table if not exists TopicKeyPhrase ( TopicId Integer, Tagger TEXT, Phrase Text, Count Integer, WordsNum Integer, Weight numeric, ExperimentId TEXT) ");
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

                logger.error("Error in insert topicPhrases: " + e);
                if (connection != null) {
                    try {
                        logger.error("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error("Error in insert topicPhrases: " + excep);
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
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e);
            }
        }

        logger.info("Finding Key phrases finished");

    }

    private void TfIdfWeighting(InstanceList instances, String SQLConnection, String experimentId, int itemType) {

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
            if (!SQLConnection.isEmpty()) {
                connection = DriverManager.getConnection(SQLConnection);
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
                    logger.error("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    logger.error("Error in insert TokensPerEntity");
                }
            }
        } finally {
            try {
                if (bulkInsert != null) {
                    bulkInsert.close();
                }
                connection.setAutoCommit(true);
            } catch (SQLException excep) {
                logger.error("Error in insert TokensPerEntity");
            }
        }

        //TODO: Sort Feature Vector Values
        // FeatureVector.toSimpFilefff
    }

    private void GenerateStoplist(SimpleTokenizer prunedTokenizer, ArrayList<Instance> instanceBuffer, int pruneCount, double docProportionMaxCutoff, boolean preserveCase)
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

        pipes.add(new Input2CharSequence()); //homer

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
        //TODO: TEST pipes.add(new FeatureSequenceRemovePlural(alphabet));

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
            String word = wordIter.next();

            if (!word.matches("^(?!.*(-[^-]*-|_[^_]*_))[A-Za-z0-9][\\w-]*[A-Za-z0-9]$") || word.length() < 3 || word.contains("cid") || word.contains("italic") || word.contains("null") || word.contains("usepackage") || word.contains("fig")) {
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

    public void createCitationGraphFile(String outputCsv, String SQLConnectionString) {
        //String SQLConnectionString = "jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";

        Connection connection = null;
        try {

            FileWriter fwrite = new FileWriter(outputCsv);
            BufferedWriter out = new BufferedWriter(fwrite);
            String header = "# DBLP citation graph \n"
                    + "# fromNodeId, toNodeId \n";
            out.write(header);

            connection = DriverManager.getConnection(SQLConnectionString);

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
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error("File input error");
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e);
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

    public void CalcEntityTopicDistributionsAndTrends(String SQLConnectionString, String experimentId) {
        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLConnectionString);
            Statement statement = connection.createStatement();

            logger.info("Calc topic Entity Topic Distributions and Trends started");

            String deleteSQL = String.format("Delete from EntityTopicDistribution where ExperimentId= '%s'", experimentId);
            statement.executeUpdate(deleteSQL);

            logger.info("Insert Full Topic Distribution ");

            String SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                    + "select '',  doc_topic.TopicId, '', 'Corpus', round(sum(weight)/SumTopicWeightView.SumWeight, 5) as NormWeight, doc_topic.ExperimentId\n"
                    + "from doc_topic\n"
                    + "INNER JOIN (SELECT  sum(weight) AS SumWeight, ExperimentId\n"
                    + "FROM doc_topic\n"
                    + "Where doc_topic.weight>0.1 \n"
                    + " and doc_topic.ExperimentId='" + experimentId + "'  \n"
                    + "GROUP BY  ExperimentId) SumTopicWeightView on SumTopicWeightView.ExperimentId= doc_topic.ExperimentId\n"
                    + "group By doc_topic.TopicId, doc_topic.ExperimentId, SumTopicWeightView.SumWeight\n"
                    + "Order by  NormWeight Desc";

            statement.executeUpdate(SQLstr);

            statement.executeUpdate(SQLstr);

            if (experimentType == ExperimentType.ACM) {

                logger.info("Trend Topic distribution for the whole coprus");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "select Document.BatchId,  doc_topic.TopicId, '', 'CorpusTrend', \n"
                        + "round(sum(weight)/SumTopicWeightPerBatchView.BatchSumWeight,5) as NormWeight,  doc_topic.ExperimentId\n"
                        + "from doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.docid and doc_topic.weight>0.1\n"
                        + "INNER JOIN (SELECT Document.BatchId, sum(weight) AS BatchSumWeight, ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document ON doc_topic.docid= Document.docid AND\n"
                        + "doc_topic.weight>0.1\n "
                        + "and doc_topic.ExperimentId='" + experimentId + "'   \n"
                        + "GROUP BY Document.BatchId, ExperimentId) SumTopicWeightPerBatchView on SumTopicWeightPerBatchView.BatchId = Document.BatchId and SumTopicWeightPerBatchView.ExperimentId= doc_topic.ExperimentId\n"
                        + "group By Document.BatchId,SumTopicWeightPerBatchView.BatchSumWeight, doc_topic.TopicId, doc_topic.ExperimentId\n"
                        + "Order by Document.BatchId,   NormWeight Desc";

                statement.executeUpdate(SQLstr);

                logger.info("Repository Topic distribution for the whole coprus");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "select '',  doc_topic.TopicId, Document.Repository, 'Repository', \n"
                        + "round(sum(weight)/SumTopicWeightPerBatchView.BatchSumWeight,5) as NormWeight,  doc_topic.ExperimentId\n"
                        + "from doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.docid and doc_topic.weight>0.1\n"
                        + "INNER JOIN (SELECT Document.Repository, sum(weight) AS BatchSumWeight, ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document ON doc_topic.docid= Document.docid AND\n"
                        + "doc_topic.weight>0.1\n "
                        + "and doc_topic.ExperimentId='" + experimentId + "'   \n"
                        + "GROUP BY Document.Repository, ExperimentId) SumTopicWeightPerBatchView on SumTopicWeightPerBatchView.Repository = Document.Repository and SumTopicWeightPerBatchView.ExperimentId= doc_topic.ExperimentId\n"
                        + "group By Document.Repository,SumTopicWeightPerBatchView.BatchSumWeight, doc_topic.TopicId, doc_topic.ExperimentId\n"
                        + "Order by Document.Repository,   NormWeight Desc";

                statement.executeUpdate(SQLstr);

                logger.info("Author Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT '', doc_topic.TopicId, Doc_author.AuthorId,'Author',\n"
                        + "                              round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                        + "                                doc_topic.ExperimentId\n"
                        + "                         FROM doc_topic\n"
                        + "                         INNER JOIN  Doc_author ON doc_topic.Docid = Doc_author.Docid AND doc_topic.weight > 0.1\n"
                        + "      and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "                              INNER JOIN (SELECT Doc_author.authorid, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "                              FROM doc_topic\n"
                        + "                              INNER JOIN   Doc_author ON doc_topic.Docid = Doc_author.Docid AND  doc_topic.weight > 0.1\n"
                        + "                              GROUP BY  ExperimentId,Doc_author.AuthorId)\n"
                        + "                              SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.AuthorId = Doc_author.AuthorId AND \n"
                        + "                                                              SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId                                        \n"
                        + "                        GROUP BY Doc_author.AuthorId,\n"
                        + "                                 SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                        + "                                 doc_topic.TopicId,\n"
                        + "                                 doc_topic.ExperimentId\n"
                        + "                                 order by  doc_topic.ExperimentId, Doc_author.AuthorId, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Journal Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT '', doc_topic.TopicId, Doc_journal.issn,'Journal',\n"
                        + "round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                        + "doc_topic.ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN  doc_journal ON doc_topic.Docid = doc_journal.Docid AND doc_topic.weight > 0.1\n"
                        + " and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "INNER JOIN (SELECT doc_journal.issn, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN   doc_journal ON doc_topic.Docid = doc_journal.Docid AND  doc_topic.weight > 0.1\n"
                        + "GROUP BY  ExperimentId,doc_journal.issn) SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.issn = doc_journal.issn AND SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId                                        \n"
                        + "GROUP BY doc_journal.issn,SumTopicWeightPerProjectView.ProjectSumWeight,doc_topic.TopicId, doc_topic.ExperimentId\n"
                        + "order by  doc_topic.ExperimentId, doc_journal.issn, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Journal Trend Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT Document.batchId, doc_topic.TopicId, doc_journal.issn,'JournalTrend',\n"
                        + "round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight, doc_topic.ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document on doc_topic.docid= document.docid and doc_topic.weight>0.1\n"
                        + " and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "INNER JOIN  doc_journal ON doc_topic.docid = doc_journal.docid                           \n"
                        + "INNER JOIN (SELECT doc_journal.issn, document.batchId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.docid and doc_topic.weight>0.1           \n"
                        + "INNER JOIN   doc_journal ON doc_topic.docid = doc_journal.docid                                \n"
                        + "GROUP BY  doc_journal.issn,Document.batchId,ExperimentId) SumTopicWeightPerProjectView \n"
                        + "ON SumTopicWeightPerProjectView.issn = doc_journal.issn AND SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId  AND SumTopicWeightPerProjectView.batchId = Document.batchId\n"
                        + "GROUP BY doc_journal.issn,Document.batchId, SumTopicWeightPerProjectView.ProjectSumWeight,doc_topic.TopicId,doc_topic.ExperimentId\n"
                        + "order by  doc_topic.ExperimentId, doc_journal.issn, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Conference Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT '', doc_topic.TopicId, doc_conference.acronymBase,'Conference',\n"
                        + "round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                        + "doc_topic.ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN  doc_conference ON doc_topic.Docid = doc_conference.Docid AND doc_topic.weight > 0.1\n"
                        + " and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "INNER JOIN (SELECT doc_conference.acronymBase, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN   doc_conference ON doc_topic.Docid = doc_conference.Docid AND  doc_topic.weight > 0.1\n"
                        + "GROUP BY  ExperimentId,doc_conference.acronymBase) SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.acronymBase = doc_conference.acronymBase AND SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId                                        \n"
                        + "GROUP BY doc_conference.acronymBase,SumTopicWeightPerProjectView.ProjectSumWeight,doc_topic.TopicId, doc_topic.ExperimentId\n"
                        + "order by  doc_topic.ExperimentId, doc_conference.acronymBase, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Conference Trend Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + " SELECT Document.batchId, doc_topic.TopicId, doc_conference.acronymBase,'ConferenceTrend',\n"
                        + "round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight, doc_topic.ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document on doc_topic.docid= document.docid and doc_topic.weight>0.1\n"
                        + " and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "INNER JOIN  doc_conference ON doc_topic.docid = doc_conference.docid                           \n"
                        + "INNER JOIN (SELECT doc_conference.acronymBase, document.batchId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.docid and doc_topic.weight>0.1           \n"
                        + "INNER JOIN   doc_conference ON doc_topic.docid = doc_conference.docid                                \n"
                        + "GROUP BY  doc_conference.acronymBase,Document.batchId,ExperimentId) SumTopicWeightPerProjectView \n"
                        + "ON SumTopicWeightPerProjectView.acronymBase = doc_conference.acronymBase AND SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId  AND SumTopicWeightPerProjectView.batchId = Document.batchId\n"
                        + "GROUP BY doc_conference.acronymBase,Document.batchId, SumTopicWeightPerProjectView.ProjectSumWeight,doc_topic.TopicId,doc_topic.ExperimentId\n"
                        + "order by  doc_topic.ExperimentId, doc_conference.acronymBase, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

            }

            if (experimentType == ExperimentType.PubMed) {

                logger.info("Trend Topic distribution for the whole coprus");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "select Document.BatchId,  doc_topic.TopicId, '', 'CorpusTrend', \n"
                        + "round(sum(weight)/SumTopicWeightPerBatchView.BatchSumWeight,5) as NormWeight,  doc_topic.ExperimentId\n"
                        + "from doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.id and doc_topic.weight>0.1\n"
                        + "INNER JOIN (SELECT Document.BatchId, sum(weight) AS BatchSumWeight, ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document ON doc_topic.docid= Document.id AND\n"
                        + "doc_topic.weight>0.1\n "
                        + "and doc_topic.ExperimentId='" + experimentId + "'   \n"
                        + "GROUP BY Document.BatchId, ExperimentId) SumTopicWeightPerBatchView on SumTopicWeightPerBatchView.BatchId = Document.BatchId and SumTopicWeightPerBatchView.ExperimentId= doc_topic.ExperimentId\n"
                        + "group By Document.BatchId,SumTopicWeightPerBatchView.BatchSumWeight, doc_topic.TopicId, doc_topic.ExperimentId\n"
                        + "Order by Document.BatchId,   NormWeight Desc";

                statement.executeUpdate(SQLstr);

                logger.info("Project Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT '', doc_topic.TopicId, Doc_Project.ProjectId,'Project',\n"
                        + "           round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                        + "             doc_topic.ExperimentId\n"
                        + "      FROM doc_topic\n"
                        + "      INNER JOIN  Doc_Project ON doc_topic.docid = Doc_Project.Docid AND doc_topic.weight > 0.1\n"
                        + "      and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "           INNER JOIN (SELECT Doc_Project.ProjectId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "           FROM doc_topic\n"
                        + "           INNER JOIN   Doc_Project ON doc_topic.docid = Doc_Project.docid AND  doc_topic.weight > 0.1\n"
                        + "           GROUP BY  ExperimentId,Doc_Project.ProjectId)\n"
                        + "           SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.ProjectId = Doc_Project.ProjectId AND \n"
                        + "                                           SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId                                            \n"
                        + "     GROUP BY Doc_Project.ProjectId,\n"
                        + "              SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                        + "              doc_topic.TopicId,\n"
                        + "              doc_topic.ExperimentId\n"
                        + "              order by  doc_topic.ExperimentId, Doc_Project.ProjectId, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Funder Topic distribution");
                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + " SELECT '', doc_topic.TopicId, doc_funder_view.funder,'Funder',\n"
                        + "                               round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight,\n"
                        + "                                 doc_topic.ExperimentId\n"
                        + "                          FROM doc_topic\n"
                        + "                          INNER JOIN  doc_funder_view ON doc_topic.docid = doc_funder_view.docid AND doc_topic.weight > 0.1\n"
                        + "                          and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "                        \n"
                        + "                               INNER JOIN (SELECT doc_funder_view.funder, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "                               FROM doc_topic\n"
                        + "                               INNER JOIN   doc_funder_view ON doc_topic.docid = doc_funder_view.docid AND  doc_topic.weight > 0.1\n"
                        + "                               \n"
                        + "                               GROUP BY  ExperimentId,doc_funder_view.funder)\n"
                        + "                               SumTopicWeightPerProjectView ON SumTopicWeightPerProjectView.funder = doc_funder_view.funder AND \n"
                        + "                                                               SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId                                            \n"
                        + "                         GROUP BY doc_funder_view.funder,\n"
                        + "                                  SumTopicWeightPerProjectView.ProjectSumWeight,\n"
                        + "                                  doc_topic.TopicId,\n"
                        + "                                  doc_topic.ExperimentId\n"
                        + "                                  order by  doc_topic.ExperimentId, doc_funder_view.funder, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

                logger.info("Funder Trend Topic distribution");

                SQLstr = "INSERT INTO EntityTopicDistribution (BatchId , TopicId ,  EntityId, EntityType,  NormWeight , ExperimentId )\n"
                        + "SELECT Document.batchId, doc_topic.TopicId, doc_funder_view.funder,'FunderTrend',\n"
                        + "round(sum(doc_topic.weight) / SumTopicWeightPerProjectView.ProjectSumWeight,5) AS NormWeight, doc_topic.ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "INNER JOIN Document on doc_topic.docid= document.id and doc_topic.weight>0.1\n"
                        + " and  doc_topic.ExperimentId='" + experimentId + "' \n"
                        + "INNER JOIN  doc_funder_view ON doc_topic.docid = doc_funder_view.docid                           \n"
                        + "INNER JOIN (SELECT doc_funder_view.funder, document.batchId, sum(weight) AS ProjectSumWeight,    ExperimentId\n"
                        + "FROM doc_topic\n"
                        + "Inner Join Document on doc_topic.docid= document.id and doc_topic.weight>0.1           \n"
                        + "INNER JOIN   doc_funder_view ON doc_topic.docid = doc_funder_view.docid                                \n"
                        + "GROUP BY  doc_funder_view.funder,Document.batchId,ExperimentId) SumTopicWeightPerProjectView \n"
                        + "ON SumTopicWeightPerProjectView.funder = doc_funder_view.funder AND SumTopicWeightPerProjectView.ExperimentId = doc_topic.ExperimentId  AND SumTopicWeightPerProjectView.batchId = Document.batchId\n"
                        + "GROUP BY doc_funder_view.funder,Document.batchId, SumTopicWeightPerProjectView.ProjectSumWeight,doc_topic.TopicId,doc_topic.ExperimentId\n"
                        + "order by  doc_topic.ExperimentId, doc_funder_view.funder, NormWeight Desc,doc_topic.ExperimentId";

                statement.executeUpdate(SQLstr);

            }
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

    public void CalcTopicSimilarities(String SQLConnectionString, String experimentId) {

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLConnectionString);
            Statement statement = connection.createStatement();

            logger.info("Calc topic similarities started");

            String distinctTopicsSQL = "Select  TopicId,  ExperimentId, count(*) as cnt\n"
                    + "from TopicVector\n  "
                    + (StringUtils.isBlank(experimentId) ? "" : String.format("where experimentId = '%s' \n  ", experimentId))
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
                        logger.error("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error("Error in insert grantSimilarity");
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
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e);
            }
        }

        logger.info("Topic similarities calculation finished");

    }

    public void calcPPRSimilarities(String SQLConnectionString) {
        //calc similarities

        //logger.info("PPRSimilarities calculation Started");
        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLConnectionString);
            connection = DriverManager.getConnection(SQLConnectionString);
            Statement statement = connection.createStatement();

            logger.info("PPRSimilarities calculation Started");

            String sql = "SELECT source.OrigId||'PPR' AS PubID, target.OrigId  AS CitationId, prLinks.Counts As Counts FROM prLinks\n"
                    + "INNER JOIN PubCitationPPRAlias source ON source.RowId = PrLinks.Source\n"
                    + "INNER JOIN PubCitationPPRAlias target ON target.RowId = PrLinks.Target\n"
                    + "Union\n"
                    + "Select Doc_id, CitationId, 1 as Counts From PubCitation\n"
                    + "ORDER by Doc_id ";

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
                newLabelId = rs.getString("Doc_id");
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

            statement.executeUpdate("create table if not exists PPRPubCitationSimilarity (Doc_id TEXT,  Similarity double) ");
            String deleteSQL = String.format("Delete from PPRPubCitationSimilarity");
            statement.executeUpdate(deleteSQL);

            PreparedStatement bulkInsert = null;
            sql = "insert into PPRPubCitationSimilarity values(?,?);";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(sql);

                for (String fromDoc_id : labelVectors.keySet()) {

                    if (fromDoc_id.contains("PPR")) {
                        continue;
                    }
                    String toDoc_id = fromDoc_id + "PPR";
                    similarity = -1;

                    if (labelVectors.get(fromDoc_id) != null && labelVectors.get(toDoc_id) != null) {
                        similarity = 1 - Math.abs(cosineSimilarity.distance(labelVectors.get(fromDoc_id), labelVectors.get(toDoc_id))); // the function returns distance not similarity
                    }
                    bulkInsert.setString(1, fromDoc_id);
                    bulkInsert.setDouble(2, (double) Math.round(similarity * 1000) / 1000);

                    bulkInsert.executeUpdate();

                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        logger.error("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error("Error in insert grantSimilarity");
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
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e);
            }
        }

        logger.info("Pub citation similarities calculation finished");
    }

    public void calcSimilarities(String SQLConnectionString, ExperimentType experimentType, String experimentId, boolean ACMAuthorSimilarity, SimilarityType similarityType, int numTopics) {
        //calc similarities

        logger.info("similarities calculation Started");
        Connection connection = null;
        try {
            // create a database connection
            //connection = DriverManager.getConnection(SQLConnectionString);
            connection = DriverManager.getConnection(SQLConnectionString);
            Statement statement = connection.createStatement();

            String sql = "";
            String entityType = "";
            switch (experimentType) {

                case PubMed:
                    entityType = "Project";
                    sql = "select EntityTopicDistribution.EntityId as projectId, EntityTopicDistribution.TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                            + "                                                        from EntityTopicDistribution                                                        \n"
                            + "                                                        where EntityTopicDistribution.EntityType='Project' \n"
                            + "                                                        AND EntityTopicDistribution.experimentId= '" + experimentId + "'    \n"
                            + "                                                        AND EntityTopicDistribution.EntityId<>'' and EntityTopicDistribution.NormWeight>0.03\n"
                            + "                                                        and EntityTopicDistribution.EntityId in (Select ProjectId FROM Doc_Project GROUP BY ProjectId HAVING Count(*)>4)\n";

                    break;
                case ACM:
                    if (ACMAuthorSimilarity) {
                        entityType = "Author";
                        sql = "select EntityTopicDistribution.EntityId as authorId, EntityTopicDistribution.TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                            from EntityTopicDistribution\n"
                                + "                            where EntityTopicDistribution.EntityType='Author' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                            EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                            and EntityTopicDistribution.EntityId in (Select AuthorId FROM doc_author GROUP BY AuthorId HAVING Count(*)>4)\n"
                                + "                            and EntityTopicDistribution.topicid in (select TopicId from topic \n"
                                + "                            where topic.experimentId='" + experimentId + "' and topic.VisibilityIndex>0)";

                    } else {
                        entityType = "JournalConference";
                        sql = "select EntityTopicDistribution.EntityId as VenueId, EntityTopicDistribution.TopicId  as TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                                                          from EntityTopicDistribution\n"
                                + "                                                          where EntityTopicDistribution.EntityType='Journal' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                                                          EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                                                          and EntityTopicDistribution.EntityId in (Select ISSN FROM doc_journal GROUP BY ISSN HAVING Count(*)>100)\n"
                                + "                                                          and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                                + "                                                          where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex=1)\n"
                                + "          UNION                                                \n"
                                + "select EntityTopicDistribution.EntityId as VenueId, EntityTopicDistribution.TopicId as TopicId, EntityTopicDistribution.NormWeight as Weight \n"
                                + "                                                          from EntityTopicDistribution\n"
                                + "                                                          where EntityTopicDistribution.EntityType='Conference' AND EntityTopicDistribution.EntityId<>'' AND\n"
                                + "                                                          EntityTopicDistribution.experimentId= '" + experimentId + "'   and EntityTopicDistribution.NormWeight>0.03\n"
                                + "                                                          and EntityTopicDistribution.EntityId in (Select SeriesId FROM doc_conference GROUP BY SeriesId HAVING Count(*)>400)\n"
                                + "                                                          and EntityTopicDistribution.topicid in (select TopicId from topicdescription \n"
                                + "                                                          where topicdescription.experimentId='" + experimentId + "' and topicdescription.VisibilityIndex=1)";
                    }

                    break;
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

                    case PubMed:
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

            //statement.executeUpdate("create table if not exists EntitySimilarity (EntityType int, EntityId1 nvarchar(50), EntityId2 nvarchar(50), Similarity double, ExperimentId nvarchar(50)) ");
            String deleteSQL = String.format("Delete from EntitySimilarity where  ExperimentId = '%s' and entityType='%s'", experimentId, entityType);
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

                                    bulkInsert.setString(1, entityType);
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
                                    bulkInsert.setString(1, entityType);
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
                        logger.error("Transaction is being rolled back");
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error("Error in insert grantSimilarity");
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
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e);
            }
        }

        logger.info("similarities calculation finished");
    }

    // Read data from DB and convert them to a list of instances per view (modality)
    // Modality 0 should be text (e.g., "This a new doc. We will tokenize it later"
    // For all other modalities (metadata) we end up with a comma delimeted string (i.e., "keyword1, key phrase1, keyword2, keyword2")
    // Some modalities may be missing for some docs
    public ArrayList<ArrayList<Instance>> ReadDataFromDB(String SQLConnection, ExperimentType experimentType, byte numModalities, int limitDocs, String filter) {
        ArrayList<ArrayList<Instance>> instanceBuffer = new ArrayList<ArrayList<Instance>>(numModalities);

        //createCitationGraphFile("C:\\projects\\Datasets\\DBLPManage\\acm_output_NET.csv", "jdbc:sqlite:C:/projects/Datasets/DBLPManage/acm_output.db");
        for (byte m = 0; m < numModalities; m++) {
            instanceBuffer.add(new ArrayList<Instance>());

        }

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLConnection);
            connection.setAutoCommit(false);

            String sql = "";
            String txtsql = "select doctxt_view.docId, text, fulltext from doctxt_view " + filter + " Order by doctxt_view.docId " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");

            if (experimentType == ExperimentType.ACM) {

                sql = " select  docid,  citations, categories, keywords, venue, DBPediaResources from docsideinfo_view " + filter + " Order by docsideinfo_view.docId " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");

                /*
                if (PPRenabled == Net2BoWType.PPR) {
                    sql = " select  docid,   citations, categories, period, keywords, venue, DBPediaResources from docsideinfo_view  Order by docsideinfo_view.docId  " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");
                } else if (PPRenabled == Net2BoWType.OneWay) {

                    sql = " select  docid,  citations, categories, keywords, venue, DBPediaResources from docsideinfo_view  Order by docsideinfo_view.docId " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");
                } else if (PPRenabled == Net2BoWType.TwoWay) {
                    sql = " select  docid, authors, citations, categories, keywords, venue, DBPediaResources from docsideinfo_view  Order by docsideinfo_view.docId  " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");

                }
                 */
            } else if (experimentType == ExperimentType.PubMed) {
                sql = " select  docid, keywords, meshterms, dbpediaresources  from docsideinfo_view  " + filter + " Order by docsideinfo_view.docId " + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");
                /* if (D4I) {
                    sql = "select distinct ON (docsideinfo_view.docid)  docsideinfo_view.docid, keywords, meshterms, dbpediaresources  \n"
                            + "from docsideinfo_view  \n"
                            + "LEFT JOIN doc_project on doc_project.docid = docsideinfo_view.docId\n"
                            + "where batchid > '2004' and (doctype='publication' OR doctype='project_report') \n"
                            + "and (repository = 'PubMed Central' OR  doc_project.projectid IN \n"
                            + "(select projectid from doc_project\n"
                            + "join document on doc_project.docid = document.id and repository = 'PubMed Central'\n"
                            + "group by projectid\n"
                            + "having count(*) > 5) )\n"
                            + "Order by docsideinfo_view.docId \n"
                            + ((limitDocs > 0) ? String.format(" LIMIT %d", limitDocs) : "");
                }*/

            }

            logger.info(" Getting text from the database");
            // get txt data 
            Statement txtstatement = connection.createStatement();
            txtstatement.setFetchSize(10000);
            ResultSet rstxt = txtstatement.executeQuery(txtsql);

            while (rstxt.next()) {

                String txt = "";

                switch (experimentType) {

                    case ACM:
                    case PubMed:
                        txt = rstxt.getString("text");
                        instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, numChars)), null, rstxt.getString("docid"), "text"));

                        break;

                    default:
                }
            }

            if (numModalities > 1) {
                logger.info(" Getting side info from the database");
                Statement statement = connection.createStatement();
                statement.setFetchSize(10000);
                ResultSet rs = statement.executeQuery(sql);

                while (rs.next()) {
                    // read the result set

                    switch (experimentType) {

                        case ACM:
//                        instanceBuffer.get(0).add(new Instance(rs.getString("Text"), null, rs.getString("pubId"), "text"));
                            //String txt = rs.getString("text");
                            //instanceBuffer.get(0).add(new Instance(txt.substring(0, Math.min(txt.length() - 1, numChars)), null, rs.getString("pubId"), "text"));

                            if (numModalities > 1) {
                                String tmpJournalStr = rs.getString("Keywords");//.replace("\t", ",");
                                if (tmpJournalStr != null && !tmpJournalStr.equals("")) {
                                    instanceBuffer.get(1).add(new Instance(tmpJournalStr.replace('-', ' ').toLowerCase(), null, rs.getString("docid"), "Keywords"));
                                }
                            }

                            if (numModalities > 2) {
                                String tmpStr = rs.getString("DBPediaResources");//.replace("\t", ",");
                                String DBPediaResourceStr = "";
                                if (tmpStr != null && !tmpStr.equals("")) {
                                    String[] DBPediaResources = tmpStr.trim().split(",");
                                    for (int j = 0; j < DBPediaResources.length; j++) {
                                        String[] pairs = DBPediaResources[j].trim().split(";");
                                        if (pairs.length == 2) {
                                            for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                                DBPediaResourceStr += pairs[0] + ",";
                                            }
                                        } else {
                                            DBPediaResourceStr += DBPediaResources[j] + ",";

                                        }
                                    }
                                    DBPediaResourceStr = DBPediaResourceStr.substring(0, DBPediaResourceStr.length() - 1);
                                    instanceBuffer.get(2).add(new Instance(DBPediaResourceStr, null, rs.getString("docid"), "DBPediaResource"));
                                }
                            }

                            if (numModalities > 3) {
                                String tmpStr = rs.getString("Categories");//.replace("\t", ",");
                                if (tmpStr != null && !tmpStr.equals("")) {

                                    instanceBuffer.get(3).add(new Instance(tmpStr, null, rs.getString("docid"), "category"));
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
                                    instanceBuffer.get(4).add(new Instance(citationStr, null, rs.getString("docid"), "citation"));
                                }
                            }

                            if (numModalities > 5) {
                                String tmpAuthorsStr = rs.getString("Venue");//.replace("\t", ",");
                                if (tmpAuthorsStr != null && !tmpAuthorsStr.equals("")) {

                                    instanceBuffer.get(5).add(new Instance(tmpAuthorsStr, null, rs.getString("docid"), "Venue"));
                                }
                            }

//DBPediaResources
                            if (numModalities > 6) {
                                String tmpAuthorsStr = rs.getString("Authors");//.replace("\t", ",");
                                if (tmpAuthorsStr != null && !tmpAuthorsStr.equals("")) {

                                    instanceBuffer.get(6).add(new Instance(tmpAuthorsStr, null, rs.getString("docid"), "author"));
                                }
                            }

                            if (numModalities > 7) {
                                String tmpPeriod = rs.getString("Period");//.replace("\t", ",");
                                if (tmpPeriod != null && !tmpPeriod.equals("")) {

                                    instanceBuffer.get(7).add(new Instance(tmpPeriod, null, rs.getString("docid"), "period"));
                                }
                            }

                            break;
                        case PubMed:
                            if (numModalities > 1) {
                                String tmpJournalStr = rs.getString("Keywords");//.replace("\t", ",");
                                if (tmpJournalStr != null && !tmpJournalStr.equals("")) {
                                    instanceBuffer.get(1).add(new Instance(tmpJournalStr.replace('-', ' ').toLowerCase(), null, rs.getString("docid"), "Keywords"));
                                }
                            }

                            if (numModalities > 2) {
                                String tmpMeshTermsStr = rs.getString("meshterms");//.replace("\t", ",");
                                if (tmpMeshTermsStr != null && !tmpMeshTermsStr.equals("")) {
                                    instanceBuffer.get(2).add(new Instance(tmpMeshTermsStr.replace('-', ' ').toLowerCase(), null, rs.getString("docid"), "MeshTerms"));
                                }
                            }

                            if (numModalities > 3) {
                                String tmpStr = rs.getString("DBPediaResources");//.replace("\t", ",");
                                //http://dbpedia.org/resource/Aerosol:3;http://dbpedia.org/resource/Growth_factor:4;http://dbpedia.org/resource/Hygroscopy:4;http://dbpedia.org/resource/Planetary_boundary_layer:3
                                String DBPediaResourceStr = "";
                                if (tmpStr != null && !tmpStr.equals("")) {
                                    String[] DBPediaResources = tmpStr.trim().split(";");
                                    for (int j = 0; j < DBPediaResources.length; j++) {
                                        String[] pairs = DBPediaResources[j].trim().split("#");
                                        if (pairs.length == 2) {
                                            for (int i = 0; i < Integer.parseInt(pairs[1]); i++) {
                                                DBPediaResourceStr += pairs[0] + ";";
                                            }
                                        } else {
                                            DBPediaResourceStr += DBPediaResources[j] + ";";

                                        }
                                    }
                                    DBPediaResourceStr = DBPediaResourceStr.substring(0, DBPediaResourceStr.length() - 1);
                                    instanceBuffer.get(3).add(new Instance(DBPediaResourceStr, null, rs.getString("docid"), "DBPediaResources"));
                                }
                            }

                            break;

                        default:
                    }

                }
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

        for (byte m = (byte) 0; m < numModalities; m++) {

            logger.info("Read " + instanceBuffer.get(m).size() + " instances modality: " + (instanceBuffer.get(m).size() > 0 ? instanceBuffer.get(m).get(0).getSource().toString() : m));

        }
        return instanceBuffer;

    }

    public InstanceList[] ImportInstancesWithExistingPipes(ArrayList<ArrayList<Instance>> instanceBuffer, Pipe[] existingPipes, byte numModalities) {
        if (existingPipes.length < numModalities) {
            logger.error("ImportDataWithExistingPipes: Missing existing pipes");
            return null;

        }

        InstanceList[] instances = new InstanceList[numModalities];

        for (byte m = 0; m < numModalities; m++) {
            instances[m] = new InstanceList(existingPipes[m]);
            instances[m].addThruPipe(instanceBuffer.get(m).iterator());
        }

        return instances;

    }

    public InstanceList[] ImportInstancesWithNewPipes(ArrayList<ArrayList<Instance>> instanceBuffer, ExperimentType experimentType, byte numModalities,
            double pruneCntPerc, double pruneLblCntPerc, double pruneMaxPerc, boolean ignoreText, String csvDelimeter) {

        InstanceList[] instances = new InstanceList[numModalities];

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
        //pipeListText.add(new FeatureSequenceRemovePlural(alphabet));
        if (!ignoreText) {
            instances[0] = new InstanceList(new SerialPipes(pipeListText));            
            
        }
        // Other Modalities
        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {
            Alphabet alphabetM = new Alphabet();
            ArrayList<Pipe> pipeListCSV = new ArrayList<Pipe>();
            pipeListCSV.add(new CSV2FeatureSequence(alphabetM, csvDelimeter));

            /*if (experimentType == ExperimentType.PubMed) {
                pipeListCSV.add(new CSV2FeatureSequence(alphabetM, ";"));
            } else {
                pipeListCSV.add(new CSV2FeatureSequence(alphabetM, ","));
            }*/
            //if (m == 1 ) //keywords
            //{
            //  pipeListCSV.add(new FeatureSequenceRemovePlural(alphabetM));
            //}
            instances[m] = new InstanceList(new SerialPipes(pipeListCSV));
        }

        if (!ignoreText) {
            try {
                int prunCnt = (int) Math.round(instanceBuffer.get(0).size() * pruneCntPerc);
                GenerateStoplist(tokenizer, instanceBuffer.get(0), prunCnt, pruneMaxPerc, false);
                instances[0].addThruPipe(instanceBuffer.get(0).iterator());
            } catch (IOException e) {
                logger.error("Problem adding text: "
                        + e);

            }
        }

        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {

            instances[m].addThruPipe(instanceBuffer.get(m).iterator());
        }

        // pruning for all other modalities no text
        for (byte m = ignoreText ? (byte) 0 : (byte) 1; m < numModalities; m++) {
            if (pruneLblCntPerc > 0 & instances[m].size() > 10) {

                // Check which type of data element the instances contain
                Instance firstInstance = instances[m].get(0);
                if (firstInstance.getData() instanceof FeatureSequence) {
                    // Version for feature sequences

                    Alphabet oldAlphabet = instances[m].getDataAlphabet();
                    Alphabet newAlphabet = new Alphabet();

                    // It's necessary to create a new instance list in
                    //  order to make sure that the data alphabet is correct.
                    SerialPipes newPipe =  new SerialPipes(((SerialPipes) instances[m].getPipe()).pipes()); 
                    //new Noop(newAlphabet, instances[m].getTargetAlphabet());
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

                        int prCnt = (int) Math.round(instanceBuffer.get(m).size() * pruneLblCntPerc);
                        fs.prune(counts, newAlphabet, ((m == 3 && experimentType == ExperimentType.PubMed)) ? prCnt * 4 : prCnt);

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
//                        logger.error("Problem serializing modality " + m + " alphabet to file "
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

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        //Class.forName("org.sqlite.JDBC");
        SciTopicFlow trainer = new SciTopicFlow();

    }
}
