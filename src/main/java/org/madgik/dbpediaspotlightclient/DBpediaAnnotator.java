/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.dbpediaspotlightclient;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.log4j.Logger;

/**
 *
 * @author omiros metaxas
 */
public class DBpediaAnnotator {

    public static Logger logger = Logger.getLogger(DBpediaAnnotator.class.getName());
    String SQLConnectionString = "jdbc:postgresql://localhost:5432/tender?user=postgres&password=postgres&ssl=false"; //"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
    String spotlightService = "";
    int numOfThreads = 4;
    double confidence = 0.4;

    public enum ExperimentType {
        PubMed,
        ACM

    }

    public enum AnnotatorType {

        spotlight,
        tagMe

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

            SQLConnectionString = prop.getProperty("SQLConnectionString");
            spotlightService = prop.getProperty("SpotlightService");
            numOfThreads = Integer.parseInt(prop.getProperty("NumOfThreads"));
            confidence = Double.parseDouble(prop.getProperty("Confidence"));

        } catch (Exception e) {
            logger.error("Exception in reading properties: " + e);

        } finally {
            inputStream.close();
        }

    }

//    public String getSQLLitedb(ExperimentType experimentType, boolean ubuntu) {
//        String SQLLitedb = "";//"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
//        //File dictPath = null;
//
//        String dbFilename = "";
//        String dictDir = "";
//        if (experimentType == ExperimentType.ACM) {
//            dbFilename = "PTMDB_ACM2016.db";
//            if (ubuntu) {
//                dictDir = ":/home/omiros/Projects/Datasets/ACM/";
//            } else {
//                dictDir = "C:\\projects\\Datasets\\ACM\\";
//            }
//        } else if (experimentType == ExperimentType.Tender) {
//            dbFilename = "PTM_Tender.db";
//            if (ubuntu) {
//                dictDir = ":/home/omiros/Projects/Datasets/PubMed/";
//            } else {
//                dictDir = "C:\\projects\\Datasets\\Tender\\";
//            }
//        } else if (experimentType == ExperimentType.OAFullGrants) {
//            dbFilename = "PTMDB_OpenAIRE.db";
//            if (ubuntu) {
//                dictDir = ":/home/omiros/Projects/Datasets/OpenAIRE/";
//            } else {
//                dictDir = "C:\\projects\\Datasets\\OpenAIRE\\";
//            }
//        } else if (experimentType == ExperimentType.LFR) {
//            dbFilename = "LFRNetMissing40.db";
//            if (ubuntu) {
//                dictDir = ":/home/omiros/Projects/Datasets/OverlappingNets/";
//            } else {
//                dictDir = "C:\\Projects\\datasets\\OverlappingNets\\LFR\\100K\\NoNoise\\";
//            }
//        }
//
//        SQLLitedb = "jdbc:sqlite:" + dictDir + dbFilename;
//        SQLLitedb = "jdbc:postgresql://localhost:5432/Tender?user=postgres&password=postgres&ssl=false"; //"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
//        
//
//        return SQLLitedb;
//    }
    public void updateResourceDetails(ExperimentType experimentType) {

        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

        // Passing it to the HttpClient.
        HttpClient httpClient = new HttpClient(connectionManager);

        logger.info(String.format("Get new resources"));

        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);

        Connection connection = null;

        int queueSize = 10000;

        BlockingQueue<String> newURIsQueue = new ArrayBlockingQueue<String>(queueSize);

        logger.info(String.format("Get extra fields from dbpedia.org using %d threads", numOfThreads));

        try {
            connection = DriverManager.getConnection(SQLConnectionString);
            String sql
                    = //"select  URI as Resource from DBpediaResource where Label=''";
                    //optimized query: hashing is much faster than seq scan
                    "select distinct Resource from doc_dbpediaResource EXCEPT select URI from DBpediaResource";

            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.setFetchSize(queueSize);

            for (int thread = 0; thread < numOfThreads; thread++) {
                executor.submit(new DBpediaAnnotatorRunnable(
                        SQLConnectionString, null,
                        null, thread, httpClient, newURIsQueue, spotlightService.replace("x", Integer.toString(thread)), confidence
                ));
            }

            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                newURIsQueue.put(rs.getString("Resource"));
            }

            for (int i = 0; i < numOfThreads; i++) {
                newURIsQueue.put(DBpediaAnnotatorRunnable.RESOURCE_POISON);
            }

        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            logger.error(e.getMessage());

        } catch (InterruptedException e) {
            logger.error("thread was interrupted, shutting down obtaining new resources phase", e);
            for (int i = 0; i < numOfThreads; i++) {
                try {
                    newURIsQueue.put(DBpediaAnnotatorRunnable.RESOURCE_POISON);
                } catch (InterruptedException e1) {
                    logger.error("got interrupted while sending poison to worker threads", e1);
                }
            }
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

        executor.shutdown();

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("execution was interrupted while awaiting submitted runnables finish", e);
        }
    }

    public void annotatePubs(ExperimentType experimentType, AnnotatorType annotator) {

        // Creating MultiThreadedHttpConnectionManager
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(20);
        connectionManager.getParams().setMaxTotalConnections(200);

        // Passing it to the HttpClient.
        HttpClient httpClient = new HttpClient(connectionManager);

        //String SQLLitedb = getSQLLitedb(experimentType, false);
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);

        Connection connection = null;

        int queueSize = 10000;

        BlockingQueue<pubText> pubsQueue = new ArrayBlockingQueue<pubText>(queueSize);

        try {
            connection = DriverManager.getConnection(SQLConnectionString);
            String sql = "";

            if (experimentType == ExperimentType.ACM) {
                sql = "select doctxt_view.docid, text,  COALESCE(keywords , ''::text)  as keywords \n"
                        + "from doctxt_view \n"
                        + "LEFT JOIN docsideinfo_view ON docsideinfo_view.docid = doctxt_view.docid \n"
                        + "LEFT JOIN doc_dbpediaresource ON doctxt_view.docid = doc_dbpediaresource.docid \n"
                        + "where doc_dbpediaresource.docid is null"; //   Limit 1000"
                ;
            } else if (experimentType == ExperimentType.PubMed) {

                // optimized query: hashing is much faster than seq scan
                sql = "select doctxt_view.docid, text,  COALESCE(keywords , ''::text) as keywords \n"
                        + "from doctxt_view \n"
                        + "LEFT JOIN ( SELECT doc_subject.docid,\n"
                        + "    string_agg(DISTINCT doc_subject.subject, ','::text) AS keywords    \n"
                        + "   FROM  doc_subject     \n"
                        + "  GROUP BY doc_subject.docid) keywords_view ON keywords_view.docid = doctxt_view.docid \n"
                        + "LEFT JOIN doc_dbpediaresource ON doctxt_view.docid = doc_dbpediaresource.docid \n"
                        + "where doc_dbpediaresource.docid is null";
            }

            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.setFetchSize(queueSize);
            logger.info("Get new publications");

            logger.info(String.format("Start annotation using %d threads, @ %s with %.2f confidence", numOfThreads, spotlightService, confidence));

            for (int thread = 0; thread < numOfThreads; thread++) {
                executor.submit(new DBpediaAnnotatorRunnable(
                        SQLConnectionString, annotator,
                        pubsQueue, thread, httpClient, null, spotlightService.replace("x", Integer.toString(thread + 1)), confidence));
            }

            ResultSet rs = statement.executeQuery(sql);
            final int logBatchSize = 100000;
            int counter = 0;

            while (rs.next()) {
                String txt = rs.getString("keywords") + "\n" + rs.getString("text");
                String pubId = rs.getString("docid");
                pubsQueue.put(new pubText(pubId, txt));
                counter++;
                if (counter % logBatchSize == 0) {
                    logger.info(String.format("Read total %s publications",
                            counter));
                }
            }

            for (int i = 0; i < numOfThreads; i++) {
                pubsQueue.put(new PubTextPoison());
            }

        } catch (SQLException e) {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error("thread was interrupted, shutting down annotation phase", e);
            for (int i = 0; i < numOfThreads; i++) {
                try {
                    pubsQueue.put(new PubTextPoison());
                } catch (InterruptedException e1) {
                    logger.error("got interrupted while sending poison to worker threads", e1);
                }
            }
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

        executor.shutdown();

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("execution was interrupted while awaiting submitted runnables finish", e);
        }
    }

    public static void main(String[] args) throws Exception {

        //Class.forName("org.sqlite.JDBC");
        Class.forName("org.postgresql.Driver");
        DBpediaAnnotator c = new DBpediaAnnotator();
        logger.info("DBPedia annotation started");
        c.getPropValues(null);
        logger.info("DBPedia annotation: Annotate new publications");
        //c.annotatePubs(ExperimentType.PubMed, AnnotatorType.spotlight);
        logger.info("DBPedia annotation: Get extra fields from DBPedia");
        c.updateResourceDetails(ExperimentType.PubMed);

    }
}
