/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.MVTopicModel;

/**
 *
 * @author omiros
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CordisDataJSONParse {

    public static Logger logger = Logger.getLogger(CordisDataJSONParse.class.getName());
    String SQLConnectionString = "jdbc:postgresql://localhost:5432/tender?user=postgres&password=postgres&ssl=false"; //"jdbc:sqlite:C:/projects/OpenAIRE/fundedarxiv.db";
    String CORDISFilesDir = "F:\\Datasets\\CORDIS\\all_cordis_data_corpus_A\\fp7_all_data";

    public class TermMention {

        public String term;
        public int mentions;

        public TermMention(String term, int mentions) {
            this.term = term;
            this.mentions = mentions;
        }
    }

    public class Project {

        public String Call;
        public String Funded_under;
        public String Project_id;
        public String Acronym;
        public String Date_from;
        public String Date_to;

        public String Report_summary;
        public String Title;
        public String Project_objective;

        public String Cordis_link;

        public List<TermMention> DiseaseList = new ArrayList<TermMention>();
        public List<TermMention> ChemicalList = new ArrayList<TermMention>();
        public List<TermMention> KeywordList = new ArrayList<TermMention>();
        public List<String> InsightList = new ArrayList<String>();
        public List<TermMention> Paper_mesh_termList = new ArrayList<TermMention>();
        public List<String> TermList = new ArrayList<String>();

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            Project guest = (Project) obj;
            return Project_id.equals(guest.Project_id);
        }

        @Override
        public int hashCode() {
            return Project_id.hashCode();
        }

        public Project(String call,
                String funded_under,
                String project_id,
                String acronym,
                String date_from,
                String date_to,
                String report_summary,
                String title,
                String project_objective,
                String cordis_link,
                List<TermMention> diseaseList,
                List<TermMention> keywordList,
                List<String> insightList,
                List<TermMention> paper_mesh_termList,
                List<String> termList,
                List<TermMention> chemicalList) {

            this.Call = call;
            this.Funded_under = funded_under;
            this.Project_id = project_id;
            this.Acronym = acronym;
            this.Date_from = date_from;
            this.Date_to = date_to;
            this.Report_summary = report_summary;
            this.Title = title;
            this.Project_objective = project_objective;
            this.Cordis_link = cordis_link;

            this.DiseaseList = diseaseList;
            this.KeywordList = keywordList;
            this.ChemicalList = chemicalList;
            this.InsightList = insightList;
            this.Paper_mesh_termList = paper_mesh_termList;
            this.TermList = termList;

        }

    }

    public static void main(String[] args) throws Exception {

        //Class.forName("org.sqlite.JDBC");
        Class.forName("org.postgresql.Driver");
        CordisDataJSONParse c = new CordisDataJSONParse();
        logger.info("CordisDataJSONParse started");
        c.getPropValues(null);
        File currentDir = new File(c.CORDISFilesDir);
        c.parseDirectoryContents(currentDir);
        logger.info("CordisDataJSONParse Finished");

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
            CORDISFilesDir = prop.getProperty("CORDISFilesDir");

        } catch (Exception e) {
            logger.error("Exception in reading properties: " + e);

        } finally {
            inputStream.close();
        }

    }

    public List<Project> parseDirectoryContents(File dir) {

        // current directory
        List<Project> projects = new ArrayList<Project>();

        try {

            File[] files = dir.listFiles();

            for (File file : files) {
                if (file.isDirectory()) {
                    logger.info("directory:" + file.getCanonicalPath());
                    parseDirectoryContents(file);
                } else {

                    logger.info("     file:" + file.getCanonicalPath());
                    String content = new String(Files.readAllBytes(Paths.get(file.getCanonicalPath())));

                    // Convert JSON string to JSONObject
                    JSONObject resultJSON = new JSONObject(content);
                    String call = resultJSON.getString("call");
                    String funded_under = resultJSON.getString("funded_under");
                    String project_id = resultJSON.getString("project_id");
                    String acronym = resultJSON.getString("acronym");
                    String date_from = resultJSON.getString("date_from");
                    String date_to = resultJSON.getString("date_to");

                    String report_summary = resultJSON.getString("report_summary");
                    String title = resultJSON.getString("title");
                    String project_objective = resultJSON.getString("project_objective");

                    String cordis_link = resultJSON.getString("cordis_link");

                    List<TermMention> diseaseList = new ArrayList<TermMention>();
                    List<TermMention> chemicalList = new ArrayList<TermMention>();
                    List<TermMention> keywordList = new ArrayList<TermMention>();
                    List<String> insightList = new ArrayList<String>();
                    List<TermMention> paper_mesh_termList = new ArrayList<TermMention>();
                    List<String> termList = new ArrayList<String>();

                    JSONArray diseases = resultJSON.optJSONArray("diseases");

                    if (diseases != null) {
                        for (int i = 0; i < diseases.length(); i++) {
                            JSONObject disease = diseases.getJSONObject(i);
                            String diseaseName = disease.getString("name");
                            int mentions = disease.getInt("mentions");
                            diseaseList.add(new TermMention(diseaseName, mentions));

                        }
                    }

                    JSONArray chemicals = resultJSON.optJSONArray("paper_chemicals");

                    if (chemicals != null) {
                        for (int i = 0; i < chemicals.length(); i++) {
                            JSONObject chemical = chemicals.getJSONObject(i);
                            String chemicalName = chemical.getString("chemical");
                            int mentions = chemical.getInt("mentions");
                            chemicalList.add(new TermMention(chemicalName, mentions));

                        }
                    }

                    JSONArray paper_keywords = resultJSON.optJSONArray("paper_keywords");

                    if (paper_keywords != null) {
                        for (int i = 0; i < paper_keywords.length(); i++) {
                            JSONObject keyword = paper_keywords.getJSONObject(i);
                            String keywordName = keyword.getString("keyword");
                            int mentions = keyword.getInt("mentions");
                            keywordList.add(new TermMention(keywordName, mentions));

                        }
                    }

                    JSONArray insights = resultJSON.optJSONArray("insights");

                    if (insights != null) {
                        for (int i = 0; i < insights.length(); i++) {
                            JSONObject insight = insights.getJSONObject(i);
                            String insightTxt = insight.getString("text");
                            insightList.add(insightTxt);

                        }
                    }

//                    JSONArray terms = resultJSON.getJSONArray("terms");
//
//                    for (int i = 0; i < insights.length(); i++) {
//                        String term = terms.getString(i);
//                        termList.add(term);
//
//                    }
                    JSONArray paper_mesh_terms = resultJSON.optJSONArray("paper_mesh_terms");

                    if (paper_mesh_terms != null) {
                        for (int i = 0; i < paper_mesh_terms.length(); i++) {
                            JSONObject paper_mesh_term = paper_mesh_terms.getJSONObject(i);
                            String term = paper_mesh_term.getString("mesh_term");
                            int mentions = paper_mesh_term.getInt("mentions");
                            paper_mesh_termList.add(new TermMention(term, mentions));

                        }
                    }

                    saveProject(SQLConnectionString, new Project(call,
                            funded_under,
                            project_id,
                            acronym,
                            date_from,
                            date_to,
                            report_summary,
                            title,
                            project_objective,
                            cordis_link,
                            diseaseList,
                            keywordList,
                            insightList,
                            paper_mesh_termList,
                            termList,
                            chemicalList));
                    //paper_mesh_terms
                    //subjects
                    //insights
                    //terms
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return projects;

    }

    public void saveProject(String connectionString, Project project) {

        Connection connection = null;
        try {

            connection = DriverManager.getConnection(connectionString);
            Statement statement = connection.createStatement();

            PreparedStatement bulkInsert = null;

            String insertSql = "INSERT INTO public.projectCORDIS(\n"
                    + "            projectid, grantid, title, acronym, funder, fundinglevel0, fundinglevel1, \n"
                    + "            fundinglevel2, callid, startdate, enddate, abstract, report, \n"
                    + "            url)\n"
                    + "    VALUES (?, ?, ?, ?, ?, ?, ?, \n"
                    + "            ?, ?, ?, ?, ?, ?, \n"
                    + "            ?) \n";

            String projectId = "CORDIS" + project.Project_id;
            try {

                connection.setAutoCommit(false);
                bulkInsert = connection.prepareStatement(insertSql);

                String fund_lvl0 = "FP7";
                String fund_lvl1 = "";
                String fund_lvl2 = project.Funded_under;

//                if (project.Funded_under.split("-").length > 1) {
//                    fund_lvl0 = project.Funded_under.split("-")[0];
//                    fund_lvl2 = project.Funded_under.split("-")[1];
//                }

                bulkInsert.setString(1, projectId);
                bulkInsert.setString(2, project.Project_id);
                bulkInsert.setString(3, project.Title);
                bulkInsert.setString(4, project.Acronym);
                bulkInsert.setString(5, "EC");
                bulkInsert.setString(6, fund_lvl0);
                bulkInsert.setString(7, fund_lvl1);
                bulkInsert.setString(8, fund_lvl2);
                bulkInsert.setString(9, project.Call);
                bulkInsert.setString(10, project.Date_from);
                bulkInsert.setString(11, project.Date_to);
                bulkInsert.setString(12, project.Project_objective);
                bulkInsert.setString(13, project.Report_summary);
                bulkInsert.setString(14, project.Cordis_link);
                int result = bulkInsert.executeUpdate();

                if (result > 0) {
                    PreparedStatement deletestatement = connection.prepareStatement("Delete from pubkeyword where pubid=?");
                    deletestatement.setString(1, projectId);
                    deletestatement.executeUpdate();

                    deletestatement = connection.prepareStatement("Delete from pubmeshterm where pubid=?");
                    deletestatement.setString(1, projectId);
                    deletestatement.executeUpdate();

                    deletestatement = connection.prepareStatement("Delete from pubkeyterm where pubid=?");
                    deletestatement.setString(1, projectId);
                    deletestatement.executeUpdate();

                    bulkInsert = null;
                    insertSql = "insert into pubkeyword (pubid, keyword) values (?,?)";
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (TermMention keyword : project.KeywordList) {
                        bulkInsert.setString(1, projectId);
                        bulkInsert.setString(2, keyword.term);
                        bulkInsert.executeUpdate();

                    }

                    bulkInsert = null;
                    insertSql = "insert into pubmeshterm (pubid, descriptor, count) values (?,?, ?)";
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (TermMention meshTerm : project.Paper_mesh_termList) {
                        bulkInsert.setString(1, projectId);
                        bulkInsert.setString(2, meshTerm.term);
                        bulkInsert.setInt(3, meshTerm.mentions);
                        bulkInsert.executeUpdate();

                    }

                    bulkInsert = null;
                    insertSql = "insert into pubkeyterm (pubid, term, type, count) values (?,?,?, ?)";
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (TermMention meshTerm : project.DiseaseList) {
                        bulkInsert.setString(1, projectId);
                        bulkInsert.setString(2, meshTerm.term);
                        bulkInsert.setString(3, "disease");
                        bulkInsert.setInt(4, meshTerm.mentions);
                        bulkInsert.executeUpdate();

                    }

                    bulkInsert = null;
                    insertSql = "insert into pubkeyterm (pubid, term, type, count) values (?,?,?, ?)";
                    bulkInsert = connection.prepareStatement(insertSql);
                    for (TermMention meshTerm : project.ChemicalList) {
                        bulkInsert.setString(1, projectId);
                        bulkInsert.setString(2, meshTerm.term);
                        bulkInsert.setString(3, "chemical");
                        bulkInsert.setInt(4, meshTerm.mentions);
                        bulkInsert.executeUpdate();

                    }

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

    }
}
