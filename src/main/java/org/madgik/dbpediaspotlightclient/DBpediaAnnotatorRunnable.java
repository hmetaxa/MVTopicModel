/**
 * Copyright 2011 Pablo Mendes, Max Jakob
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.madgik.dbpediaspotlightclient;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator.AnnotatorType;
/**
 * Simple web service-based annotation client for DBpedia Spotlight.
 *
 * @author pablomendes, Joachim Daiber
 */
public class DBpediaAnnotatorRunnable implements Runnable {

    public static Logger logger = Logger.getLogger(DBpediaAnnotator.class.getName());
    int startDoc, numDocs;
    List<pubText> pubs = null;
    List<String> resources = null;
    String SQLLitedb = "";
    AnnotatorType annotator;
    int threadId = 0;
    boolean annotate = true;    
    String spotlightService;
    double confidence; 

    //private HttpClient client = new HttpClient();
    private final HttpClient httpClient;

    public DBpediaAnnotatorRunnable(
            int startDoc, int numDocs, String SQLLitedb, AnnotatorType annotator,
            List<pubText> pubs, int threadId, HttpClient httpClient, List<String> resources, boolean annotate, String spotlightService, double confidence ) {
        this.numDocs = numDocs;
        this.startDoc = startDoc;
        this.pubs = pubs;
        this.SQLLitedb = SQLLitedb;
        this.annotator = annotator;
        this.threadId = threadId;
        this.httpClient = httpClient;
        this.annotate = annotate;
        this.resources = resources;
        this.spotlightService = spotlightService;
        this.confidence = confidence;
        
    }

    public void getAndUpdateDetails(String resourceURI) {

        String response = "";

        String query = "prefix dbpedia-owl: <http://dbpedia.org/ontology/>\n"
                + "prefix dcterms: <http://purl.org/dc/terms/>                              \n"
                + "                \n"
                + "                                 SELECT  ?label ?subject ?subjectLabel ?redirect ?redirectLabel ?disambiguates ?disambiguatesLabel ?abstract\n"
                + "                                 WHERE {\n"
                + "                                     ?uri dbpedia-owl:abstract ?abstract .\n"
                + "                                     ?uri rdfs:label ?label .\n"
                + "OPTIONAL{\n"
                + "                                     ?uri dcterms:subject ?subject.\n"
                + "                                     ?subject rdfs:label ?subjectLabel .\n"
                + "}\n"
                + "OPTIONAL{\n"
                + "                                     ?disambiguates  dbpedia-owl:wikiPageDisambiguates  ?uri .\n"
                + "                                     ?disambiguates rdfs:label ?disambiguatesLabel.\n"
                + "}\n"
                + "OPTIONAL{\n"
                + "                                     ?redirect dbpedia-owl:wikiPageRedirects ?uri .                    \n"
                + "                                     ?redirect rdfs:label ?redirectLabel .\n"
                + "}\n"
                + "                                     FILTER (?uri = <" + resourceURI + "> && langMatches(lang(?label),\"en\")  \n"
                + "                                             && langMatches(lang(?abstract),\"en\") && langMatches(lang(?disambiguatesLabel),\"en\")\n"
                + "                                            && langMatches(lang(?redirectLabel ),\"en\") && langMatches(lang(?subjectLabel ),\"en\") )         \n"
                + "                                   \n"
                + "                                 }";

        try {
            String searchUrl = "http://dbpedia.org/sparql?"
                    + "query=" + URLEncoder.encode(query, "utf-8")
                    + "&format=json";

            GetMethod getMethod = new GetMethod(searchUrl);

            //getMethod.addRequestHeader(new Header("Accept", "application/json"));
            response = request(getMethod);

        } catch (Exception e) {
            logger.error("Invalid response from dbpedia API:" + e);
            //throw new Exception("Received invalid response from DBpedia Spotlight API.");

        }

        assert response != null;

        JSONObject resultJSON = null;
        JSONArray entities = null;

        try {
            resultJSON = new JSONObject(response);
            entities = resultJSON.getJSONObject("results").getJSONArray("bindings");
        } catch (JSONException e) {
            logger.error("Invalid JSON response from dbpedia API:" + e);
            
        }

        if (entities != null) {
            Set<DBpediaLink> categories = new HashSet<DBpediaLink>();
            Set<DBpediaLink> abreviations = new HashSet<DBpediaLink>();
            String resourceAbstract = "";
            String label = "";
            for (int i = 0; i < entities.length(); i++) {
                try {
                    JSONObject entity = entities.getJSONObject(i);

                    try {
                        categories.add(new DBpediaLink(entity.getJSONObject("subject").getString("value"), entity.getJSONObject("subjectLabel").getString("value")));
                    } catch (JSONException e) {
                        logger.debug("JSON parsing categories not found:" + e);
                        
                    }
                    try {
                        String redirectLabel = entity.getJSONObject("redirectLabel").getString("value");

                        if (redirectLabel.toUpperCase().equals(redirectLabel)) {
                            abreviations.add(new DBpediaLink(entity.getJSONObject("redirect").getString("value"), redirectLabel));
                        }
                    } catch (JSONException e) {
                        logger.debug("JSON parsing redirectLabel not found:" + e);
                        
                    }
                    try {
                        String disambiguatesLabel = entity.getJSONObject("disambiguatesLabel").getString("value").replace("(disambiguation)", "").trim();
                        if (disambiguatesLabel.toUpperCase().equals(disambiguatesLabel)) {
                            abreviations.add(new DBpediaLink(entity.getJSONObject("disambiguates").getString("value"), disambiguatesLabel));
                        }
                    } catch (JSONException e) {
                       logger.debug("JSON parsing disambiguatesLabel not found:" + e);
                        
                    }
                    if (i == 0) {
                        resourceAbstract = entity.getJSONObject("abstract").getString("value");
                        label = entity.getJSONObject("label").getString("value");
                    }

                } catch (JSONException e) {
                    logger.error("JSON parsing exception from dbpedia API:" + e);
                    
                }

            }

            //public DBpediaResource(DBpediaResourceType type, String URI, String title, int support,  double Similarity, double confidence, String mention, List<String> categories, String wikiAbstract, String wikiId) {
            saveResourceDetails( new DBpediaResource(DBpediaResourceType.Entity, resourceURI, label, 0, 1,
                    1, "", categories, resourceAbstract, "", abreviations));
        }
    }

    public List<DBpediaResource> extractFromSpotlight(String input) throws Exception {

        //private final static String API_URL = "http://localhost:2222/rest/candidates";
        //final String API_URL = "http://localhost:2222/rest/annotate";
        final String API_URL = spotlightService.trim(); //"http://model.dbpedia-spotlight.org/en/annotate";
        //http://model.dbpedia-spotlight.org/en/annotate
        //private final static String API_URL = "http://www.dbpedia-spotlight.com/en/annotate";
        //private final static String API_URL = "http://spotlight.sztaki.hu:2222/rest/annotate";
        //final double CONFIDENCE = 0.4;
        final int SUPPORT = 0;

        String spotlightResponse = "";
        LinkedList<DBpediaResource> resources = new LinkedList<DBpediaResource>();
        try {
            GetMethod getMethod = new GetMethod(API_URL + "/?"
                    + "confidence=" + confidence
                    + "&support=" + SUPPORT
                    + "&text=" //President%20Obama%20called%20Wednesday%20on%20Congress%20to%20extend%20a%20tax%20break%20for%20students%20included%20in%20last%20year%27s%20economic%20stimulus%20package,%20arguing%20that%20the%20policy%20provides%20more%20generous%20assistance"
                    //+ URLEncoder.encode("President Obama called Wednesday on Congress to extend a tax break for students included in last year's economic stimulus package, arguing that the policy provides more generous assistance", "utf-8")
                    + URLEncoder.encode(input, "utf-8")
            );

            getMethod.addRequestHeader(new Header("Accept", "application/json"));

            spotlightResponse = request(getMethod);

        } catch (UnsupportedEncodingException e) {
        }

        assert spotlightResponse != null;

        JSONObject resultJSON = null;
        JSONArray entities = null;

        try {
            resultJSON = new JSONObject(spotlightResponse);
            entities = resultJSON.getJSONArray("Resources");
        } catch (JSONException e) {
            //FIXME this is pretty common when no resources were found, not an error though. Log level changed from error to debug. We should check spotlightResponse details and show an appropriate error then.
            logger.debug(String.format("Invalid response -no resources- from DBpedia Spotlight API for input %s: %s", input, e));
            return resources;
            
        }

        JSONObject entity = null;
        for (int i = 0; i < entities.length(); i++) {
            try {
                entity = entities.getJSONObject(i);
                //public DBpediaResource(DBpediaResourceType type, String URI, String title, int support,  double Similarity, double confidence, String mention, List<String> categories, String wikiAbstract, String wikiId) {
                resources.add(
                        new DBpediaResource(DBpediaResourceType.Entity, entity.getString("@URI"), "", Integer.parseInt(entity.getString("@support")), Double.parseDouble(entity.getString("@similarityScore")),
                                1, entity.getString("@surfaceForm"), null, "", "", null));

            } catch (JSONException e) {
                logger.error(String.format("Invalid response -no details- from DBpedia Spotlight API for resource %s: %s", entity.toString(), e));
                
                
            }

        }

        return resources;

    }

    public List<DBpediaResource> extractFromTagMe(String input) throws Exception {

        //https://tagme.d4science.org/tagme/tag?lang=en&&include_abstract=true&include_categories=true&gcube-token=27edab24-27a7-4e51-a335-1d5356342cab-843339462&text=latent%20dirichlet%20allocation
        //private final static String API_URL = "http://localhost:2222/rest/candidates";
        final String API_URL = "http://tagme.d4science.org/tagme/tag?lang=en&&include_abstract=true&include_categories=true&gcube-token=27edab24-27a7-4e51-a335-1d5356342cab-843339462&text=";
        //&long_text=30
        String response = "";
        LinkedList<DBpediaResource> resources = new LinkedList<DBpediaResource>();
        try {
            GetMethod getMethod = new GetMethod(API_URL
                    + URLEncoder.encode(input, "utf-8")
            );

            //getMethod.addRequestHeader(new Header("Accept", "application/json"));
            response = request(getMethod);

        } catch (UnsupportedEncodingException e) {
            logger.error("UnsupportedEncodingException calling TagMe API:" + e);
        }

        assert response != null;

        JSONObject resultJSON = null;
        JSONArray entities = null;

        /*
        {"timestamp":"2016-12-02T11:36:49","time":0,"test":"5","api":"tag",
        "annotations":[{"abstract":"In natural language processing, Latent Dirichlet allocation (LDA) is a generative statistical model that allows sets of observations to be explained by unobserved groups that explain why some parts of the data are similar. For example, if observations are words collected into documents, it posits that each document is a mixture of a small number of topics and that each word's creation is attributable to one of the document's topics. LDA is an example of a topic model and was first presented as a graphical model for topic discovery by David Blei, Andrew Ng, and Michael I. Jordan in 2003.",
        "id":4605351,
        "title":"Latent Dirichlet allocation",
        "dbpedia_categories":["Statistical natural language processing","Latent variable models","Probabilistic models"],
        "start":0,
        "link_probability":1,
        "rho":0.5,"end":27,"spot":"latent dirichlet allocation"}],"lang":"en"}
         */
        try {
            resultJSON = new JSONObject(response);
            entities = resultJSON.getJSONArray("annotations");
        } catch (JSONException e) {
            logger.error("Invalid response from DBpedia TagMe API:" + e);
            return resources;
            //throw new Exception("Received invalid response from DBpedia Spotlight API. \n");
        }

        for (int i = 0; i < entities.length(); i++) {
            try {
                JSONObject entity = entities.getJSONObject(i);
                Set<DBpediaLink> categories = new HashSet<DBpediaLink>();

                try {
                    JSONArray JSONcategories = entity.getJSONArray("dbpedia_categories");

                    for (int j = 0; j < JSONcategories.length(); j++) {
                        categories.add(new DBpediaLink(JSONcategories.getString(j), ""));
                    }
                } catch (JSONException e) {
                    logger.error("Invalid JSON response from TagMe API:" + e);
                    //LOG.error("JSON exception "+e);
                }
                //public DBpediaResource(DBpediaResourceType type, String URI, String title, int support,  double Similarity, double confidence, String mention, List<String> categories, String wikiAbstract, String wikiId) {
                DBpediaResource newResource = new DBpediaResource(DBpediaResourceType.Entity, "", entity.getString("title"), 0, entity.getDouble("link_probability"),
                        entity.getDouble("rho"), entity.getString("spot"), categories, entity.getString("abstract"), String.valueOf(entity.getInt("id")), null);

                resources.add(newResource);

            } catch (JSONException e) {
                logger.error("Invalid JSON response from TagMe API:" + e);
                
            }

        }

        return resources;

    }

    public void run() {

        if (annotate && pubs != null) {
            for (int doc = startDoc;
                    doc < pubs.size() && doc < startDoc + numDocs;
                    doc++) {
                pubText pub = pubs.get(doc);
                List<DBpediaResource> entities = getDBpediaEntities(pub.getText(), annotator, doc - startDoc);
                saveDBpediaEntities(SQLLitedb, entities, pub.getPubId(), annotator);
                
            }
        } else if (resources != null) {
            for (int resource = startDoc;
                    resource < resources.size() && resource < startDoc + numDocs;
                    resource++) {

                final long startDocTime = System.currentTimeMillis();
                getAndUpdateDetails(resources.get(resource));
                final long endDocTime = System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s]: Extraction time for %s resource: %s ms  \n", threadId, resource, (endDocTime - startDocTime)));    
                }
            }
        }
    }

    protected static String readFileAsString(String filePath) throws java.io.IOException {
        return readFileAsString(new File(filePath));
    }

    protected static String readFileAsString(File file) throws IOException {
        byte[] buffer = new byte[(int) file.length()];
        BufferedInputStream f = new BufferedInputStream(new FileInputStream(file));
        f.read(buffer);
        return new String(buffer);

    }

    static abstract class LineParser {

        public abstract String parse(String s) throws ParseException;

        static class ManualDatasetLineParser extends LineParser {

            public String parse(String s) throws ParseException {
                return s.trim();
            }
        }

        static class OccTSVLineParser extends LineParser {

            public String parse(String s) throws ParseException {
                String result = s;
                try {
                    result = s.trim().split("\t")[3];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new ParseException(e.getMessage(), 3);
                }
                return result;
            }
        }
    }

    public void saveDBpediaEntities(String SQLLitedb, List<DBpediaResource> entities, String pubId, AnnotatorType annotator) {

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLLitedb);
            Statement statement = connection.createStatement();

            PreparedStatement bulkInsert = null;
            /*
             PubId      TEXT,
    Resource   TEXT,
    Support    INT,
    Count      INT     DEFAULT (1),
    Similarity NUMERIC,
    Mention    TEXT,
    Confidence DOUBLE,
    Annotator  TEXT,
             */
            
//          SQLite  String insertSql = "insert into pubDBpediaResource (pubId, Resource, Support,  similarity,  mention,confidence, annotator, count ) values (?,?,?,?,?,?,?, \n"
//                    + "    ifnull((select count from pubDBpediaResource where pubId = ? and Resource=? and mention=?), 0) + 1)";
//         
            
            String insertSql ="insert into pubDBpediaResource (pubId, Resource, Support,  similarity,  mention,confidence, annotator, count ) values (?,?,?,?,?,?,?,0)\n" +
"ON CONFLICT (pubId, resource,mention) DO UPDATE SET \n" +
"support=EXCLUDED.Support,\n" +
"similarity=EXCLUDED.similarity, \n" +
" confidence=EXCLUDED.confidence, \n" +
" annotator=EXCLUDED.annotator, \n" +
" count=pubDBpediaResource.count+1";

            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(insertSql);
                for (DBpediaResource e : entities) {

                    String resource = annotator == AnnotatorType.spotlight ? e.getLink().uri : e.getLink().label;

                    bulkInsert.setString(1, pubId);
                    bulkInsert.setString(2, resource);
                    bulkInsert.setInt(3, e.getSupport());
                    bulkInsert.setDouble(4, e.getSimilarity());
                    bulkInsert.setString(5, e.getMention());
                    bulkInsert.setDouble(6, e.getConfidence());
                    bulkInsert.setString(7, annotator.name());

                    //SQlite bulkInsert.setString(8, pubId);
                    //SQlite bulkInsert.setString(9, resource);
                    //SQlite bulkInsert.setString(10, e.getMention());

                    bulkInsert.executeUpdate();

                }

                connection.commit();

            } catch (SQLException e) {

                if (connection != null) {
                    try {
                        logger.error("Transaction is being rolled back:" + e.toString());
                        connection.rollback();
                    } catch (SQLException excep) {
                        logger.error("Error in insert pubDBpediaResource:" + e.toString());
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

        if (annotator == AnnotatorType.tagMe) {
            for (DBpediaResource e : entities) {
                saveResourceDetails( e);
            }
        }

    }

    public void saveResourceDetails(DBpediaResource resource) {

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(SQLLitedb);
            //Statement statement = connection.createStatement();

            //INSERT OR IGNORE INTO EVENTTYPE (EventTypeName) VALUES 'ANI Received'
            //statement.executeUpdate(String.format("Update pubDBpediaResource Set abstract='%s' where URI='%s'", resourceAbstract, URI));
            //SQLITE String myStatement = " insert or ignore into DBpediaResource (Id, URI, label, wikiId, abstract) Values (?, ?, ?, ?, ?) ";
            String myStatement = " insert into DBpediaResource (Id, URI, label, wikiId, abstract) Values (?, ?, ?, ?, ?) ON CONFLICT (Id) DO NOTHING ";
            PreparedStatement statement = connection.prepareStatement(myStatement);
            String id = resource.getLink().uri.isEmpty() ? resource.getLink().label : resource.getLink().uri;

            statement.setString(1, id);
            statement.setString(2, resource.getLink().uri);
            statement.setString(3, resource.getLink().label);
            statement.setString(4, resource.getWikiId());
            statement.setString(5, resource.getWikiAbstract());
            int result = statement.executeUpdate();

            if (result > 0) {
                PreparedStatement deletestatement = connection.prepareStatement("Delete from DBpediaResourceCategory where ResourceId=?");
                deletestatement.setString(1, id);
                deletestatement.executeUpdate();

                deletestatement = connection.prepareStatement("Delete from DBpediaResourceAcronym where ResourceId=?");
                deletestatement.setString(1, id);
                deletestatement.executeUpdate();

                PreparedStatement bulkInsert = null;

                String insertSql = "insert into DBpediaResourceCategory (ResourceId, CategoryLabel, CategoryURI) values (?,?, ?)";

                try {

                    connection.setAutoCommit(false);
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (DBpediaLink category : resource.getCategories()) {
                        bulkInsert.setString(1, id);
                        bulkInsert.setString(2, category.label);
                        bulkInsert.setString(3, category.uri);
                        bulkInsert.executeUpdate();

                    }

                    connection.commit();

                } catch (SQLException e) {

                    if (connection != null) {
                        try {
                            logger.error("Transaction is being rolled back:" + e.toString());
                            connection.rollback();
                            connection.setAutoCommit(true);
                        } catch (SQLException excep) {
                            logger.error("Error in insert DBpediaResource Category:" + excep.toString());
                        }
                    }
                } finally {

                    if (bulkInsert != null) {
                        bulkInsert.close();
                    }
                    connection.setAutoCommit(true);
                }

                insertSql = "insert into DBpediaResourceAcronym (ResourceId, AcronymLabel, AcronymURI) values (?,?, ?)";

                try {

                    connection.setAutoCommit(false);
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (DBpediaLink acronym : resource.getAbreviations()) {
                        bulkInsert.setString(1, id);
                        bulkInsert.setString(2, acronym.label);
                        bulkInsert.setString(3, acronym.uri);
                        bulkInsert.executeUpdate();

                    }

                    connection.commit();

                } catch (SQLException e) {

                    if (connection != null) {
                        try {
                            logger.error("Transaction is being rolled back:" + e.toString());
                            connection.rollback();
                            connection.setAutoCommit(true);
                        } catch (SQLException excep) {
                            logger.error("Error in insert DBpediaResource Acronym:" + excep.toString());
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

    }

    public List<DBpediaResource> getDBpediaEntities(String text, AnnotatorType annotator, int docNum) {

        LineParser parser = new LineParser.ManualDatasetLineParser();
        List<DBpediaResource> entities = new ArrayList<DBpediaResource>();
        int correct = 0;
        int error = 0;
        int sum = 0;
        int i = 0;

        String txt2Annotate = "";
        int txt2AnnotatNum = 0;
        final long startDocTime = System.currentTimeMillis();
        String[] txts = text.split("\n");
        for (String snippet : txts) {
            String s = "";
            try {
                s = parser.parse(snippet);
            } catch (Exception e) {

                logger.error(e.toString());
            }
            if (s != null && !s.equals("")) {
                i++;
                txt2Annotate += (" " + s);

                if ((i % 5) == 0 || i == txts.length) {
                    txt2AnnotatNum++;

                    try {
                        final long startTime = System.currentTimeMillis();
                        if (annotator == AnnotatorType.spotlight) {
                            entities.addAll(extractFromSpotlight(txt2Annotate.replaceAll("\\s+", " ")));
                        } else if (annotator == AnnotatorType.tagMe) {
                            entities.addAll(extractFromTagMe(txt2Annotate.replaceAll("\\s+", " ")));
                        }

                        final long endTime = System.currentTimeMillis();
                        sum += (endTime - startTime);
                        
                        correct++;
                    } catch (Exception e) {
                        error++;
                        logger.error(e.toString());
                        
                        e.printStackTrace();
                    }

                    txt2Annotate = "";
                }

            }

        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s]: Extracted %s entities from %s text items, with %s successes and %s errors. \n", threadId, entities.size(), txt2AnnotatNum, correct, error));
            double avg = (new Double(sum) / txt2AnnotatNum);
            final long endDocTime = System.currentTimeMillis();
            logger.debug(String.format("[%s]: Extraction time for %s pub: Total:%s ms AvgPerRequest:%s ms \n", threadId, docNum, (endDocTime - startDocTime), avg));
        }
        return entities;
    }

    public void saveExtractedEntitiesSet(File inputFile, File outputFile, LineParser parser, int restartFrom, AnnotatorType annotator) throws Exception {
        PrintWriter out = new PrintWriter(outputFile);
        //LOG.info("Opening input file "+inputFile.getAbsolutePath());
        String text = readFileAsString(inputFile);
        int i = 0;
        int correct = 0;
        int error = 0;
        int sum = 0;
        for (String snippet : text.split("\n")) {
            String s = parser.parse(snippet);
            if (s != null && !s.equals("")) {
                i++;

                if (i < restartFrom) {
                    continue;
                }

                List<DBpediaResource> entities = new ArrayList<DBpediaResource>();
                try {
                    final long startTime = System.nanoTime();
                    if (annotator == AnnotatorType.spotlight) {
                        entities.addAll(extractFromSpotlight(snippet.replaceAll("\\s+", " ")));
                    } else if (annotator == AnnotatorType.tagMe) {
                        entities.addAll(extractFromTagMe(snippet.replaceAll("\\s+", " ")));
                    }
                    final long endTime = System.nanoTime();
                    sum += endTime - startTime;
                    logger.info(String.format("(%s) Extraction ran in %s ns. \n", i, endTime - startTime));
                    
                    correct++;
                } catch (Exception e) {
                    error++;
                    logger.error(e);
                    
                    
                }
                for (DBpediaResource e : entities) {
                    out.println(e.getLink().uri);
                }
                out.println();
                out.flush();
            }
        }
        out.close();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Extracted entities from %s text items, with %s successes and %s errors. \n", i, correct, error));
            logger.debug("Results saved to: " + outputFile.getAbsolutePath());
            double avg = (new Double(sum) / i);
            logger.debug(String.format("Average extraction time: %s ms \n", avg * 1000000));
        }
    }

    public void evaluate(File inputFile, File outputFile) throws Exception {
        evaluateManual(inputFile, outputFile, 0);
    }

    public void evaluateManual(File inputFile, File outputFile, int restartFrom) throws Exception {
        saveExtractedEntitiesSet(inputFile, outputFile, new LineParser.ManualDatasetLineParser(), restartFrom, AnnotatorType.spotlight);
    }
    
    private static String getStringFromInputStream(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
                    logger.error(e);
			
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error(e);
				}
			}
		}

		return sb.toString();

	}
    

    public String request(HttpMethod method) throws Exception {

        String spotlightResponse = null;

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = httpClient.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                return String.valueOf(statusCode);
            }

            // Read the response body.
            InputStream responseBody = method.getResponseBodyAsStream(); // .getResponseBody(); //TODO Going to buffer response body of large or unknown size. Using getResponseBodyAsStream instead is recommended.

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            spotlightResponse = getStringFromInputStream(responseBody);

        } catch (HttpException e) {

            throw new Exception("Protocol error executing HTTP request.", e);
        } catch (IOException e) {

            throw new Exception("Transport error executing HTTP request.", e);
        } finally {
            // Release the connection.
            method.releaseConnection();
        }

        return spotlightResponse;

    }

}
