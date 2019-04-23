package org.madgik.MVTopicModel;

import org.madgik.utils.MixTopicModelTopicAssignment;
import java.io.*;
import java.util.*;
import java.text.*;

import cc.mallet.types.*;
import cc.mallet.util.*;

import gnu.trove.*;
import gnu.trove.set.hash.TIntHashSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class FastQMVWVTopicModelDiagnostics {

    int numTopics;
    int numTopWords;
    public static final int TWO_PERCENT_INDEX = 1;
    public static final int FIFTY_PERCENT_INDEX = 6;
    public static final double[] DEFAULT_DOC_PROPORTIONS = {0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.5};
    public static Logger logger = Logger.getLogger("SciTopic");
    /**
     * All words in sorted order, with counts
     */
    //ArrayList<TreeSet<IDSorter>> topicSortedWords;

    ArrayList<ArrayList<TreeSet<IDSorter>>> topicSortedWords; //= new ArrayList<ArrayList<TreeSet<IDSorter>>>(numModalities);

    /**
     * The top N words in each topic in an array for easy access
     */
    String[][] topicTopWords;
    ArrayList<TopicScores> diagnostics;
    FastQMVWVParallelTopicModel model;
    Alphabet alphabet;
    int[][][] topicCodocumentMatrices;
    int[] numRank1Documents;
    int[] numNonZeroDocuments;
    int[][] numDocumentsAtProportions;
    // This quantity is used in entropy calculation
    double[] sumCountTimesLogCount;
    int[] wordTypeCounts;
    int numTokens = 0;
    int[] tokensPerTopicArray;

    public FastQMVWVTopicModelDiagnostics(FastQMVWVParallelTopicModel model, int numTopWords) {
        numTopics = model.getNumTopics();
        this.numTopWords = numTopWords;

        this.model = model;

        alphabet = model.getAlphabet()[0];

        topicSortedWords = new ArrayList<ArrayList<TreeSet<IDSorter>>>(model.numModalities);
        for (Byte m = 0; m < model.numModalities; m++) {
            topicSortedWords.add(model.getSortedWords(m));
        }
        //topicSortedWords = model.getSortedWords(0);

        topicTopWords = new String[numTopics][numTopWords];

        numRank1Documents = new int[numTopics];
        numNonZeroDocuments = new int[numTopics];
        numDocumentsAtProportions = new int[numTopics][DEFAULT_DOC_PROPORTIONS.length];
        sumCountTimesLogCount = new double[numTopics];

        diagnostics = new ArrayList<TopicScores>();

        for (int topic = 0; topic < numTopics; topic++) {

            int position = 0;
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            // How many words should we report? Some topics may have fewer than
            //  the default number of words with non-zero weight.
            int limit = numTopWords;
            if (sortedWords.size() < numTopWords) {
                limit = sortedWords.size();
            }

            Iterator<IDSorter> iterator = sortedWords.iterator();
            for (int i = 0; i < limit; i++) {
                IDSorter info = iterator.next();
                topicTopWords[topic][i] = (String) alphabet.lookupObject(info.getID());
            }

        }

        collectDocumentStatistics();
        
        //int[] tokensPerTopicIntArray = new int[numTopics];
        tokensPerTopicArray = new int[model.tokensPerTopic[0].length()];
        for (int pi = 0; pi < model.tokensPerTopic[0].length(); pi++) {
            tokensPerTopicArray[pi] = model.tokensPerTopic[0].get(pi);
        }

        diagnostics.add(getTokensPerTopic(tokensPerTopicArray));
        diagnostics.add(getDocumentEntropy(tokensPerTopicArray));
        diagnostics.add(getWordLengthScores());
        diagnostics.add(getCoherence());
        diagnostics.add(getDiscrCoherence());
        diagnostics.add(getDiscrWeightWithinTopics());
        diagnostics.add(getDistanceFromUniform());
        diagnostics.add(getDistanceFromCorpus());
        diagnostics.add(getEffectiveNumberOfWords());
        diagnostics.add(getTokenDocumentDiscrepancies());
        diagnostics.add(getRank1Percent());
        diagnostics.add(getDocumentPercentRatio(FIFTY_PERCENT_INDEX, TWO_PERCENT_INDEX));
        diagnostics.add(getDocumentPercent(5));

    }

    public void collectDocumentStatistics() {

        topicCodocumentMatrices = new int[numTopics][numTopWords][numTopWords];
        wordTypeCounts = new int[alphabet.size()];
        numTokens = 0;

        // This is an array of hash sets containing the words-of-interest for each topic,
        //  used for checking if the word at some position is one of those words.
        TIntHashSet[] topicTopWordIndices = new TIntHashSet[numTopics];

        // The same as the topic top words, but with int indices instead of strings,
        //  used for iterating over positions.
        int[][] topicWordIndicesInOrder = new int[numTopics][numTopWords];

        // This is an array of hash sets that will hold the words-of-interest present in a document,
        //  which will be cleared after every document.
        TIntHashSet[] docTopicWordIndices = new TIntHashSet[numTopics];

        int numDocs = model.getData().size();

        // The count of each topic, again cleared after every document.
        int[] topicCounts = new int[numTopics];

        for (int topic = 0; topic < numTopics; topic++) {
            TIntHashSet wordIndices = new TIntHashSet();

            for (int i = 0; i < numTopWords; i++) {
                if (topicTopWords[topic][i] != null) {
                    int type = alphabet.lookupIndex(topicTopWords[topic][i]);
                    topicWordIndicesInOrder[topic][i] = type;
                    wordIndices.add(type);
                }
            }

            topicTopWordIndices[topic] = wordIndices;
            docTopicWordIndices[topic] = new TIntHashSet();
        }

        int doc = 0;

        for (MixTopicModelTopicAssignment document : model.getData()) {

            if (document.Assignments[0] != null) {
                FeatureSequence tokens = (FeatureSequence) document.Assignments[0].instance.getData();
                FeatureSequence topics = (FeatureSequence) document.Assignments[0].topicSequence;

                for (int position = 0; position < tokens.size(); position++) {
                    int type = tokens.getIndexAtPosition(position);
                    int topic = topics.getIndexAtPosition(position);

                    numTokens++;
                    wordTypeCounts[type]++;

                    topicCounts[topic]++;

                    if (topicTopWordIndices[topic].contains(type)) {
                        docTopicWordIndices[topic].add(type);
                    }
                }

                int docLength = tokens.size();

                if (docLength > 0) {
                    int maxTopic = -1;
                    int maxCount = -1;

                    for (int topic = 0; topic < numTopics; topic++) {

                        if (topicCounts[topic] > 0) {
                            numNonZeroDocuments[topic]++;

                            if (topicCounts[topic] > maxCount) {
                                maxTopic = topic;
                                maxCount = topicCounts[topic];
                            }

                            sumCountTimesLogCount[topic] += topicCounts[topic] * Math.log(topicCounts[topic]);

                            double proportion = (model.gamma[0] * model.alpha[0][topic] + topicCounts[topic]) / ((double) model.gamma[0] * model.alphaSum[0] + docLength);
                            for (int i = 0; i < DEFAULT_DOC_PROPORTIONS.length; i++) {
                                if (proportion < DEFAULT_DOC_PROPORTIONS[i]) {
                                    break;
                                }
                                numDocumentsAtProportions[topic][i]++;
                            }

                            TIntHashSet supportedWords = docTopicWordIndices[topic];
                            int[] indices = topicWordIndicesInOrder[topic];

                            for (int i = 0; i < numTopWords; i++) {
                                if (supportedWords.contains(indices[i])) {
                                    for (int j = i; j < numTopWords; j++) {
                                        if (i == j) {
                                            // Diagonals are total number of documents with word W in topic T
                                            topicCodocumentMatrices[topic][i][i]++;
                                        } else if (supportedWords.contains(indices[j])) {
                                            topicCodocumentMatrices[topic][i][j]++;
                                            topicCodocumentMatrices[topic][j][i]++;
                                        }
                                    }
                                }
                            }

                            docTopicWordIndices[topic].clear();
                            topicCounts[topic] = 0;
                        }
                    }

                    if (maxTopic > -1) {
                        numRank1Documents[maxTopic]++;
                    }
                }

                doc++;
            }
        }
    }

    public int[][] getCodocumentMatrix(int topic) {
        return topicCodocumentMatrices[topic];
    }

    public TopicScores getTokensPerTopic(int[] tokensPerTopic) {
        TopicScores scores = new TopicScores("tokens", numTopics, numTopWords);

        for (int topic = 0; topic < numTopics; topic++) {
            scores.setTopicScore(topic, tokensPerTopic[topic]);
        }

        return scores;
    }

    public TopicScores getDocumentEntropy(int[] tokensPerTopic) {
        TopicScores scores = new TopicScores("document_entropy", numTopics, numTopWords);

        for (int topic = 0; topic < numTopics; topic++) {
            scores.setTopicScore(topic, -sumCountTimesLogCount[topic] / tokensPerTopic[topic] + Math.log(tokensPerTopic[topic]));
        }

        return scores;
    }

    public TopicScores getDistanceFromUniform() {
        int[] tokensPerTopic = tokensPerTopicArray;

        TopicScores scores = new TopicScores("uniform_dist", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        int numTypes = alphabet.size();

        for (int topic = 0; topic < numTopics; topic++) {

            double topicScore = 0.0;
            int position = 0;
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            for (IDSorter info : sortedWords) {
                int type = info.getID();
                double count = info.getWeight();

                double score = (count / tokensPerTopic[topic])
                        * Math.log((count * numTypes) / tokensPerTopic[topic]);

                if (position < numTopWords) {
                    scores.setTopicWordScore(topic, position, score);
                }

                topicScore += score;
                position++;
            }

            scores.setTopicScore(topic, topicScore);
        }

        return scores;
    }

    public TopicScores getDiscrWeightWithinTopics() {

        double[][] topicsDiscrWeight = model.calcDiscrWeightWithinTopics(topicSortedWords, true);
        TopicScores scores = new TopicScores("discrWeight", numTopics, numTopWords);
        for (int kk = 0; kk < numTopics; kk++) {
            if (model.alpha[0][kk] != 0) {

                scores.setTopicScore(kk, topicsDiscrWeight[0][kk]);

            }

        }

        return scores;
    }

    public TopicScores getDiscrCoherence() {

        double avgAlpha = 0;
        int cnt = 0;

        for (int kk = 0; kk <= numTopics; kk++) {
            //int k = kactive.get(kk);
            avgAlpha += model.alpha[0][kk];
            cnt += model.alpha[0][kk] == 0 ? 0 : 1;

        }

        avgAlpha = avgAlpha / cnt; /// (numTopics - inActiveTopicIndex.size());
        double[][] topicsDiscrWeight = model.calcDiscrWeightWithinTopics(topicSortedWords, true);
        TopicScores scores = new TopicScores("normDiscrWeight", numTopics, numTopWords);
        for (int kk = 0; kk < numTopics; kk++) {
            if (model.alpha[0][kk] != 0) {
                double diffLogWeight = Math.abs(Math.log10(model.alpha[0][kk]) - Math.log10(avgAlpha));
                scores.setTopicScore(kk, topicsDiscrWeight[0][kk] / diffLogWeight);

            }

        }

        return scores;
    }

    public TopicScores getEffectiveNumberOfWords() {
        int[] tokensPerTopic = tokensPerTopicArray;

        TopicScores scores = new TopicScores("eff_num_words", numTopics, numTopWords);

        int numTypes = alphabet.size();

        for (int topic = 0; topic < numTopics; topic++) {

            double sumSquaredProbabilities = 0.0;
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            for (IDSorter info : sortedWords) {
                int type = info.getID();
                double probability = info.getWeight() / tokensPerTopic[topic];

                sumSquaredProbabilities += probability * probability;
            }

            scores.setTopicScore(topic, 1.0 / sumSquaredProbabilities);
        }

        return scores;
    }

    /**
     * Low-quality topics may be very similar to the global distribution.
     */
    public TopicScores getDistanceFromCorpus() {

        int[] tokensPerTopic = tokensPerTopicArray;

        TopicScores scores = new TopicScores("corpus_dist", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        for (int topic = 0; topic < numTopics; topic++) {

            double coefficient = (double) numTokens / tokensPerTopic[topic];

            double topicScore = 0.0;
            int position = 0;
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            for (IDSorter info : sortedWords) {
                int type = info.getID();
                double count = info.getWeight();

                double score = (count / tokensPerTopic[topic])
                        * Math.log(coefficient * count / wordTypeCounts[type]);

                if (position < numTopWords) {
                    //System.out.println(alphabet.lookupObject(type) + ": " + count + " * " + numTokens + " / " + wordTypeCounts[type] + " * " + tokensPerTopic[topic] + " = " + (coefficient * count / wordTypeCounts[type]));
                    scores.setTopicWordScore(topic, position, score);
                }

                topicScore += score;

                position++;
            }

            scores.setTopicScore(topic, topicScore);
        }

        return scores;
    }

    public TopicScores getTokenDocumentDiscrepancies() {
        TopicScores scores = new TopicScores("token-doc-diff", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        for (int topic = 0; topic < numTopics; topic++) {
            int[][] matrix = topicCodocumentMatrices[topic];
            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            double topicScore = 0.0;

            double[] wordDistribution = new double[numTopWords];
            double[] docDistribution = new double[numTopWords];

            double wordSum = 0.0;
            double docSum = 0.0;

            int position = 0;
            Iterator<IDSorter> iterator = sortedWords.iterator();
            while (iterator.hasNext() && position < numTopWords) {
                IDSorter info = iterator.next();

                wordDistribution[position] = info.getWeight();
                docDistribution[position] = matrix[position][position];

                wordSum += wordDistribution[position];
                docSum += docDistribution[position];

                position++;
            }

            for (position = 0; position < numTopWords; position++) {
                double p = wordDistribution[position] / wordSum;
                double q = docDistribution[position] / docSum;
                double meanProb = 0.5 * (p + q);

                double score = 0.0;
                if (p > 0) {
                    score += 0.5 * p * Math.log(p / meanProb);
                }
                if (q > 0) {
                    score += 0.5 * q * Math.log(q / meanProb);
                }

                scores.setTopicWordScore(topic, position, score);
                topicScore += score;
            }

            scores.setTopicScore(topic, topicScore);
        }

        return scores;
    }

    /**
     * Low-quality topics often have lots of unusually short words.
     */
    public TopicScores getWordLengthScores() {

        TopicScores scores = new TopicScores("word-length", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        for (int topic = 0; topic < numTopics; topic++) {
            int total = 0;
            for (int position = 0; position < topicTopWords[topic].length; position++) {
                if (topicTopWords[topic][position] == null) {
                    break;
                }

                int length = topicTopWords[topic][position].length();
                total += length;

                scores.setTopicWordScore(topic, position, length);
            }
            scores.setTopicScore(topic, (double) total / topicTopWords[topic].length);
        }

        return scores;
    }

    /**
     * Low-quality topics often have lots of unusually short words.
     */
    public TopicScores getWordLengthStandardDeviation() {

        TopicScores scores = new TopicScores("word-length-sd", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        // Get the mean length
        double meanLength = 0.0;
        int totalWords = 0;

        for (int topic = 0; topic < numTopics; topic++) {
            for (int position = 0; position < topicTopWords[topic].length; position++) {
                // Some topics may not have all N words
                if (topicTopWords[topic][position] == null) {
                    break;
                }
                meanLength += topicTopWords[topic][position].length();
                totalWords++;
            }
        }

        meanLength /= totalWords;

        // Now calculate the standard deviation
        double lengthVariance = 0.0;

        for (int topic = 0; topic < numTopics; topic++) {
            for (int position = 0; position < topicTopWords[topic].length; position++) {
                if (topicTopWords[topic][position] == null) {
                    break;
                }

                int length = topicTopWords[topic][position].length();

                lengthVariance += (length - meanLength) * (length - meanLength);
            }
        }
        lengthVariance /= (totalWords - 1);

        // Finally produce an overall topic score
        double lengthSD = Math.sqrt(lengthVariance);
        for (int topic = 0; topic < numTopics; topic++) {
            for (int position = 0; position < topicTopWords[topic].length; position++) {
                if (topicTopWords[topic][position] == null) {
                    break;
                }

                int length = topicTopWords[topic][position].length();

                scores.addToTopicScore(topic, (length - meanLength) / lengthSD);
                scores.setTopicWordScore(topic, position, (length - meanLength) / lengthSD);
            }
        }

        return scores;
    }

    public TopicScores getCoherence() {
        TopicScores scores = new TopicScores("coherence", numTopics, numTopWords);
        scores.wordScoresDefined = true;

        for (int topic = 0; topic < numTopics; topic++) {
            int[][] matrix = topicCodocumentMatrices[topic];

            double topicScore = 0.0;

            for (int row = 0; row < numTopWords; row++) {
                double rowScore = 0.0;
                double minScore = 0.0;
                for (int col = 0; col < row; col++) {
                    double score = Math.log((matrix[row][col] + model.beta[0]) / (matrix[col][col] + model.beta[0]));
                    rowScore += score;
                    if (score < minScore) {
                        minScore = score;
                    }
                }
                topicScore += rowScore;
                scores.setTopicWordScore(topic, row, minScore);
            }

            scores.setTopicScore(topic, topicScore);
        }

        return scores;
    }

    public TopicScores getRank1Percent() {
        TopicScores scores = new TopicScores("rank_1_docs", numTopics, numTopWords);

        for (int topic = 0; topic < numTopics; topic++) {
            scores.setTopicScore(topic, (double) numRank1Documents[topic] / numNonZeroDocuments[topic]);
        }

        return scores;
    }

    public TopicScores getDocumentPercentRatio(int numeratorIndex, int denominatorIndex) {
        TopicScores scores = new TopicScores("allocation_ratio", numTopics, numTopWords);

        if (numeratorIndex > numDocumentsAtProportions[0].length || denominatorIndex > numDocumentsAtProportions[0].length) {
            System.err.println("Invalid proportion indices (max " + (numDocumentsAtProportions[0].length - 1) + ") : "
                    + numeratorIndex + ", " + denominatorIndex);
            return scores;
        }

        for (int topic = 0; topic < numTopics; topic++) {
            scores.setTopicScore(topic, (double) numDocumentsAtProportions[topic][numeratorIndex]
                    / numDocumentsAtProportions[topic][denominatorIndex]);
        }

        return scores;
    }

    public TopicScores getDocumentPercent(int i) {
        TopicScores scores = new TopicScores("allocation_count", numTopics, numTopWords);

        if (i > numDocumentsAtProportions[0].length) {
            System.err.println("Invalid proportion indices (max " + (numDocumentsAtProportions[0].length - 1) + ") : " + i);
            return scores;
        }

        for (int topic = 0; topic < numTopics; topic++) {
            scores.setTopicScore(topic, (double) numDocumentsAtProportions[topic][i] / numNonZeroDocuments[topic]);
        }

        return scores;
    }

    public void saveToDB(String SQLLitedb, String experimentId, double perplexity, String BatchId) {
        //String SQLLitedb = "jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";

        Connection connection = null;
        String current_score = "";
        try {

            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            PreparedStatement bulkInsert = null;
            String sql = "insert into expDiagnostics values(?,?,?,?,?,?);";

            connection.setAutoCommit(false);
            bulkInsert = connection.prepareStatement(sql);

            for (byte m = 0; m < model.numModalities; m++) {
                current_score =  String.format("perplexity %d" , m);

                bulkInsert.setString(1, experimentId);
                bulkInsert.setString(2, BatchId);
                bulkInsert.setString(3, "TestCorpus");
                bulkInsert.setInt(4, 0); //corpus
                bulkInsert.setString(5, "perplexity");
                bulkInsert.setDouble(6, perplexity);
                bulkInsert.executeUpdate();

                int p = 1;
                while (p < model.perplexities[m].length && model.perplexities[m][p] != 0) //for (int p = 0; p < model.perplexities[m].length; p++) 
                {
                    current_score =  String.format("LogLikehood %d %d", m,p);
                    bulkInsert.setString(1, experimentId);
                    bulkInsert.setString(2, BatchId);
                    bulkInsert.setString(3, String.format("%d", 10 * p));
                    bulkInsert.setInt(4, 0); //corpus
                    bulkInsert.setString(5, "LogLikehood");
                    bulkInsert.setDouble(6, model.perplexities[m][p]);
                    bulkInsert.executeUpdate();
                    p++;
                }

//                p = 1;
//                while (p < model.convergenceRates[m].length && model.convergenceRates[m][p] > 0) //                for (int p = 0; p < model.convergenceRates[m].length; p++) 
//                {
//                    bulkInsert.setString(1, experimentId);
//                    bulkInsert.setString(2, String.format("%d", 10 * p));
//                    bulkInsert.setInt(3, 0); //corpus
//                    bulkInsert.setString(4, "convergenceRates");
//                    bulkInsert.setDouble(5, model.convergenceRates[m][p]);
//                    bulkInsert.executeUpdate();
//                    p++;
//                }
            }

            for (int topic = 0; topic < numTopics; topic++) {

                for (TopicScores scores : diagnostics) {
                    current_score = scores.name+String.format(" Topic %d", topic);
                    
                    bulkInsert.setString(1, experimentId);
                    bulkInsert.setString(2, BatchId);
                    bulkInsert.setString(3, String.format("Topic %d", topic));
                    bulkInsert.setInt(4, 1); //Topic
                    bulkInsert.setString(5, scores.name);
                    bulkInsert.setDouble(6, scores.scores[topic]);
                    bulkInsert.executeUpdate();
                }

//                for (int position = 0; position < topicTopWords[topic].length; position++) {
//                    if (topicTopWords[topic][position] == null) {
//                        break;
//                    }
//
//                    formatter.format("  %s", topicTopWords[topic][position]);
//                    for (TopicScores scores : diagnostics) {
//                        if (scores.wordScoresDefined) {
//                            formatter.format("\t%s=%.4f", scores.name, scores.topicWordScores[topic][position]);
//                            bulkInsert.setString(1, experimentId);
//                    bulkInsert.setString(2, String.format("Topic %d", topic));
//                    bulkInsert.setInt(3, 1); //Word
//                    bulkInsert.setString(4, scores.name);
//                    bulkInsert.setDouble(5, scores.scores[topic]);
//                    bulkInsert.executeUpdate();
//                        }
//                    }
//                    
//                }
            }

            connection.commit();
            if (bulkInsert != null) {
                bulkInsert.close();
            }
            connection.setAutoCommit(true);

        } catch (SQLException e) {

            logger.error("Exception in save diagnostics score ["+ current_score +"] : "+ e.getMessage());
            
            if (connection != null) {
                try {
                    logger.error("Transaction is being rolled back \n");
                    connection.rollback();
                } catch (SQLException excep) {
                    logger.error("Error in insert expDiagnostics \n");
                }
            }
        } finally {
        }

    }

    public String toString() {

        StringBuilder out = new StringBuilder();
        Formatter formatter = new Formatter(out, Locale.US);

        for (int topic = 0; topic < numTopics; topic++) {

            formatter.format("Topic %d", topic);

            for (TopicScores scores : diagnostics) {
                formatter.format("\t%s=%.4f", scores.name, scores.scores[topic]);
            }
            formatter.format("\n");

            for (int position = 0; position < topicTopWords[topic].length; position++) {
                if (topicTopWords[topic][position] == null) {
                    break;
                }

                formatter.format("  %s", topicTopWords[topic][position]);
                for (TopicScores scores : diagnostics) {
                    if (scores.wordScoresDefined) {
                        formatter.format("\t%s=%.4f", scores.name, scores.topicWordScores[topic][position]);
                    }
                }
                out.append("\n");
            }
        }

        return out.toString();
    }

    public String toXML() {

        int[] tokensPerTopic = tokensPerTopicArray;

        StringBuilder out = new StringBuilder();
        Formatter formatter = new Formatter(out, Locale.US);

        out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        out.append("<model>\n");

        for (int topic = 0; topic < numTopics; topic++) {

            int[][] matrix = topicCodocumentMatrices[topic];

            formatter.format("<topic id='%d'", topic);

            for (TopicScores scores : diagnostics) {
                formatter.format(" %s='%.4f'", scores.name, scores.scores[topic]);
            }
            out.append(">\n");

            TreeSet<IDSorter> sortedWords = topicSortedWords.get(0).get(topic);

            // How many words should we report? Some topics may have fewer than
            //  the default number of words with non-zero weight.
            int limit = numTopWords;
            if (sortedWords.size() < numTopWords) {
                limit = sortedWords.size();
            }

            double cumulativeProbability = 0.0;

            Iterator<IDSorter> iterator = sortedWords.iterator();
            for (int position = 0; position < limit; position++) {
                IDSorter info = iterator.next();
                double probability = info.getWeight() / tokensPerTopic[topic];
                cumulativeProbability += probability;

                formatter.format("<word rank='%d' count='%.0f' prob='%.5f' cumulative='%.5f' docs='%d'", position + 1, info.getWeight(), probability, cumulativeProbability, matrix[position][position]);

                for (TopicScores scores : diagnostics) {
                    if (scores.wordScoresDefined) {
                        formatter.format(" %s='%.4f'", scores.name, scores.topicWordScores[topic][position]);
                    }
                }
                formatter.format(">%s</word>\n", topicTopWords[topic][position]);
            }

            out.append("</topic>\n");
        }
        out.append("</model>\n");

        return out.toString();
    }

    public class TopicScores {

        public String name;
        public double[] scores;
        public double[][] topicWordScores;
        /**
         * Some diagnostics have meaningful values for each word, others do not
         */
        public boolean wordScoresDefined = false;

        public TopicScores(String name, int numTopics, int numWords) {
            this.name = name;
            scores = new double[numTopics];
            topicWordScores = new double[numTopics][numWords];
        }

        public void setTopicScore(int topic, double score) {
            scores[topic] = score;
        }

        public void addToTopicScore(int topic, double score) {
            scores[topic] += score;
        }

        public void setTopicWordScore(int topic, int wordPosition, double score) {
            topicWordScores[topic][wordPosition] = score;
            wordScoresDefined = true;
        }
    }

    public static void main(String[] args) throws Exception {
        InstanceList[] training = new InstanceList[1];
        training[0] = InstanceList.load(new File(args[0]));
        int numTopics = Integer.parseInt(args[1]);
        byte mod = 1;
        FastQMVWVParallelTopicModel model = new FastQMVWVParallelTopicModel(numTopics, mod, 0.1, 0.01, true, "", true, 0.6, true);
        model.addInstances(training, "", 1, "");
        model.setNumIterations(1000);

        model.estimate();

        FastQMVWVTopicModelDiagnostics diagnostics = new FastQMVWVTopicModelDiagnostics(model, 20);

        if (args.length == 3) {
            PrintWriter out = new PrintWriter(args[2]);
            out.println(diagnostics.toXML());
            out.close();
        }
    }
}
