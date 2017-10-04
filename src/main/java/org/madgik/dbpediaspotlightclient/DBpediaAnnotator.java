/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.dbpediaspotlightclient;

import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author omiros
 */
public class DBpediaAnnotator {

    public enum ExperimentType {
        OpenAIRE,
        ACM,
        OAFullGrants,
        OAFETGrants,
        Tender,
        LFR
    }

    public enum AnnotatorType {

        spotlight,
        tagMe

    }

    public String getSQLLitedb(ExperimentType experimentType, boolean ubuntu) {
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
            dbFilename = "LFRNetMissing40.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/OverlappingNets/";
            } else {
                dictDir = "C:\\Projects\\datasets\\OverlappingNets\\LFR\\100K\\NoNoise\\";
            }
        }

        SQLLitedb = "jdbc:sqlite:" + dictDir + dbFilename;
        SQLLitedb = "jdbc:postgresql://localhost:5432/Tender?user=postgres&password=postgres&ssl=false"; //"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
        

        return SQLLitedb;
    }

    public List<String> getNewResources(ExperimentType experimentType, String SQLLitedb) {
        List<String> URIs = new ArrayList<String>();

        //String SQLLitedb = getSQLLitedb(experimentType, false);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(SQLLitedb);
            String sql = 
                    //"select  URI as Resource from DBpediaResource where Label=''";
                    "select distinct Resource from pubDBpediaResource where Resource not in (select URI from DBpediaResource) ";

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(60);  // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {

                String URI = rs.getString("Resource");

                URIs.add(URI);

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
        return URIs;

    }

    public void updateResourceDetails(ExperimentType experimentType, int numThreads) {

        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

        // Passing it to the HttpClient.
        HttpClient httpClient = new HttpClient(connectionManager);

        String SQLLitedb = getSQLLitedb(experimentType, false);

        List<String> newURIs = getNewResources(experimentType, SQLLitedb);
        //List<String> newURIs = new ArrayList<String>(); // getNewResources(experimentType, SQLLitedb);
        //newURIs.add("http://dbpedia.org/resource/Artificial_intelligence");

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        DBpediaAnnotatorRunnable[] runnables = new DBpediaAnnotatorRunnable[numThreads];

        int docsPerThread = newURIs.size() / numThreads;
        int offset = 0;

        for (int thread = 0; thread < numThreads; thread++) {

            // some docs may be missing at the end due to integer division
            if (thread == numThreads - 1) {
                docsPerThread = newURIs.size() - offset;
            }

            runnables[thread] = new DBpediaAnnotatorRunnable(
                    offset, docsPerThread, SQLLitedb, null,
                    null, thread, httpClient, newURIs, false, experimentType
            );

            offset += docsPerThread;

        }

        for (int thread = 0; thread < numThreads; thread++) {

            executor.submit(runnables[thread]);

        }
        
        executor.shutdown();

    }

    public void annotatePubs(ExperimentType experimentType, AnnotatorType annotator, int numThreads) {

        // Creating MultiThreadedHttpConnectionManager
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

        // Passing it to the HttpClient.
        HttpClient httpClient = new HttpClient(connectionManager);

        List<pubText> pubs = new ArrayList<pubText>();

        String SQLLitedb = getSQLLitedb(experimentType, false);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(SQLLitedb);
            String sql = "";

            if (experimentType == ExperimentType.ACM) {
                sql = " SELECT Publication.PubId AS pubId,\n"
                        + "       Publication.title || ' ' || IFNULL(Publication.abstract, '')||' ' || substr(IFNULL(pubFullText.fulltext, ''), 300, 15000)  AS text,     \n"
                        + "       GROUP_CONCAT(DISTINCT PubKeyword.Keyword) AS keywords    \n"
                        + "      FROM Publication\n"
                        + "           LEFT OUTER JOIN  pubFullText ON pubFullText.PubId = publication.PubId            \n"
                        + "           LEFT OUTER JOIN  PubKeyword ON PubKeyword.PubId = publication.pubId\n"
                        + "           WHERE NOT (IFNULL(pubFullText.fulltext, '') = '' AND IFNULL(Publication.abstract, '') = '')\n"
                        + " AND Publication.PubId NOT IN (select distinct pubId from pubdbpediaresource) \n"
                        + "     GROUP BY Publication.pubId\n" //+"     Limit 1000"
                        ;
            } else if (experimentType == ExperimentType.Tender) {
                sql = " select pubId, TEXT, keywords, Grants from PubView WHERE  PubView.PubId NOT IN (select distinct pubId from pubdbpediaresource)";// LIMIT 100000";
            } else if (experimentType == ExperimentType.OAFullGrants) {
                sql = " select pubId, TEXT, GrantIds, Funders, Areas, AreasDescr, Venue from OpenAIREPubView";// LIMIT 100000";
            }
            else if (experimentType == ExperimentType.OpenAIRE) {
                sql = "select pubId, text, fulltext, keywords from pubview limit 10";// LIMIT 100000";
            }
            

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(60);  // set timeout to 30 sec.

            //statement.executeUpdate("create table if not exists PubDBpediaResource (PubId TEXT, ResourceURI TEXT, Support INT) ");
            //String deleteSQL = String.format("Delete from PubDBpediaResource");
            //statement.executeUpdate(deleteSQL);
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {

                String txt = rs.getString("keywords") + "\n" + rs.getString("text");
                String pubId = rs.getString("pubId");

                pubs.add(new pubText(pubId, txt));

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

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        DBpediaAnnotatorRunnable[] runnables = new DBpediaAnnotatorRunnable[numThreads];

        int docsPerThread = pubs.size() / numThreads;
        int offset = 0;

        for (int thread = 0; thread < numThreads; thread++) {

            // some docs may be missing at the end due to integer division
            if (thread == numThreads - 1) {
                docsPerThread = pubs.size() - offset;
            }

            runnables[thread] = new DBpediaAnnotatorRunnable(
                    offset, docsPerThread, SQLLitedb, annotator,
                    pubs, thread, httpClient, null, true, experimentType
            );

            offset += docsPerThread;

        }

        for (int thread = 0; thread < numThreads; thread++) {

            executor.submit(runnables[thread]);

        }

        executor.shutdown();
    }

    public static void main(String[] args) throws Exception {

        //Class.forName("org.sqlite.JDBC");
        Class.forName("org.postgresql.Driver");
        DBpediaAnnotator c = new DBpediaAnnotator();
        c.annotatePubs(ExperimentType.OpenAIRE, AnnotatorType.spotlight, 2);
        c.updateResourceDetails(ExperimentType.Tender, 2);

    }
}
