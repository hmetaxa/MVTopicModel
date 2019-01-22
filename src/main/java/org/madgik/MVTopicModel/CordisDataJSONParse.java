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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
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

    public class Publication {

        public String pmId;
        public String pmcId;
        public String DOI;
        //public String Date;
        public String Year;
        public String Title;
        public String Abstract;
        public String Repository;
        public List<String> MeSHTerms;
        //public List<TermMention> DiseaseList;
        //public List<TermMention> ChemicalList;
        public List<String> KeywordList;
//        public List<String> InsightList;

        public Publication(String pmcId,
                String pmId,
                String DOI,
                //String Date,
                String Year,
                String Title,
                String Abstract,
                List<String> MeSHTerms,
                //List<TermMention> DiseaseList,
                //List<TermMention> ChemicalList,
                List<String> KeywordList
        //        List<String> InsightList
        ) {
            this.pmId = pmId;
            this.pmcId = pmcId;
            this.DOI = DOI;
            //this.Date = Date;
            this.Year = Year;
            this.Title = Title;
            this.Abstract = Abstract;
            this.MeSHTerms = MeSHTerms;
            //this.DiseaseList = DiseaseList;
            //this.ChemicalList = ChemicalList;
            this.KeywordList = KeywordList;
            //this.InsightList = InsightList;

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

        public List<Publication> PublicationList;

        //public List<TermMention> DiseaseList;
        //public List<TermMention> ChemicalList;
        //public List<TermMention> KeywordList;
        //public List<String> InsightList;
        //public List<TermMention> Paper_mesh_termList;
        //public List<String> TermList;
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
                //List<TermMention> diseaseList,
                //List<TermMention> keywordList,
                //List<String> insightList,
                //List<TermMention> paper_mesh_termList,
                //List<String> termList,
                //List<TermMention> chemicalList,
                List<Publication> publicationList) {

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

            /*this.DiseaseList = diseaseList;
            this.KeywordList = keywordList;
            this.ChemicalList = chemicalList;
            this.InsightList = insightList;
            this.Paper_mesh_termList = paper_mesh_termList;
            this.TermList = termList;*/
            this.PublicationList = publicationList;

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
                    JSONObject administrativeData = resultJSON.getJSONObject("administrative_data");

                    String call = administrativeData.getString("call");
                    String funded_under = administrativeData.getString("funded_under");
                    String project_id = administrativeData.getString("project_id");
                    String acronym = administrativeData.getString("acronym");
                    String date_from = administrativeData.getString("date_from");
                    String date_to = administrativeData.getString("date_to");
                    String year = date_from.split("-")[0];
                    String cordis_link = administrativeData.getString("cordis_link");

                    JSONObject sections = resultJSON.getJSONObject("sections");
                    String title = "";
                    String project_objective = "";
                    String report_summary = "";
                    List<String> sectionsList = new ArrayList<String>();

                    for (Iterator key = sections.keys(); key.hasNext();) {
                        String name = key.next().toString();
                        JSONObject section = sections.getJSONObject(name);
                        String text = section.getString("text");
                        if (name.equals("title")) {
                            title = text;
                        } else if (name.equals("1")) {
                            project_objective = text;
                            sectionsList.add(name + "::" + text);
                        } else {
                            int len = Math.min(text.length() - 1, 10000);
                            report_summary += text.substring(0, len); // Get only the first 10.000
                            sectionsList.add(name + "::" + text.substring(0, len));
                        }
                    }
                    List<Publication> publicationList = new ArrayList<Publication>();

                    for (String section : sectionsList) {
                        String[] txts = section.split("::");
                        String type = txts[0];
                        String txt = txts[1];

                        publicationList.add(new Publication(
                                "CORDIS" + project_id + type,
                                "",
                                "", //DOI
                                year,
                                title + " " + type,
                                txt, //Abstract
                                null,
                                null));
                    }

                    JSONObject publications = resultJSON.getJSONObject("publications").getJSONObject("pubmed_abstracts");

                    for (Iterator key = publications.keys(); key.hasNext();) {
                        String pmid = key.next().toString();
                        JSONObject publication = publications.getJSONObject(pmid);

                        String pubTitle = publication.optString("ArticleTitle","");
                        String abstractText = publication.optString("AbstractText","");
                        String pubDate = publication.optString("ArticleDate", "1/1/2016");
                        String pubYear = pubDate.split("/")[2];

                        JSONArray otherIds = publication.optJSONArray("OtherIDs");
                        String doi = "";
                        String pmcid = "";
                        if (otherIds != null) {
                            for (int i = 0; i < otherIds.length(); i++) {
                                JSONObject paper_mesh_term = otherIds.getJSONObject(i);
                                String source = paper_mesh_term.getString("Source");
                                if (source.equals("doi")) {
                                    doi = paper_mesh_term.getString("id");

                                }
                                if (source.equals("pmc")) {
                                    pmcid = paper_mesh_term.getString("id");

                                }

                            }
                        }

                        List<String> keywords = new ArrayList<String>();
                        JSONArray keywordList = publication.optJSONArray("Keywords");

                        if (keywordList != null) {
                            for (int i = 0; i < keywordList.length(); i++) {
                                keywords.add(keywordList.getString(i));

                            }
                        }

                        JSONArray paper_mesh_terms = publication.optJSONArray("MeshHeadings");

                        List<String> meshTerms = new ArrayList<String>();
                        if (paper_mesh_terms != null) {
                            for (int i = 0; i < paper_mesh_terms.length(); i++) {
                                JSONArray meshTermsArray = paper_mesh_terms.getJSONArray(i);
                                for (int j = 0; j < meshTermsArray.length(); j++) {
                                    JSONObject paper_mesh_term = meshTermsArray.getJSONObject(j);
                                    String label = paper_mesh_term.getString("Label");
                                    if (label.equals("DescriptorName")) {
                                        String term = paper_mesh_term.getString("text");
                                        meshTerms.add(term);

                                    }
                                }

                            }
                        }

                        publicationList.add(new Publication(
                                pmcid,
                                pmid,
                                doi, //DOI
                                pubYear,
                                pubTitle,
                                abstractText, //Abstract
                                meshTerms,
                                keywords));
                    }

//                    List<TermMention> diseaseList = new ArrayList<TermMention>();
//                    List<TermMention> chemicalList = new ArrayList<TermMention>();
//                    List<TermMention> keywordList = new ArrayList<TermMention>();
//                    List<String> insightList = new ArrayList<String>();
//                    List<TermMention> paper_mesh_termList = new ArrayList<TermMention>();
//                    List<String> termList = new ArrayList<String>();
//
//                    List<Publication> publicationList = new ArrayList<Publication>();
//
//                    JSONArray diseases = resultJSON.optJSONArray("diseases");
//
//                    if (diseases != null) {
//                        for (int i = 0; i < diseases.length(); i++) {
//                            JSONObject disease = diseases.getJSONObject(i);
//                            String diseaseName = disease.getString("name");
//                            int mentions = disease.getInt("mentions");
//                            diseaseList.add(new TermMention(diseaseName, mentions));
//
//                        }
//                    }
//
//                    JSONArray chemicals = resultJSON.optJSONArray("paper_chemicals");
//
//                    if (chemicals != null) {
//                        for (int i = 0; i < chemicals.length(); i++) {
//                            JSONObject chemical = chemicals.getJSONObject(i);
//                            String chemicalName = chemical.getString("chemical");
//                            int mentions = chemical.getInt("mentions");
//                            chemicalList.add(new TermMention(chemicalName, mentions));
//
//                        }
//                    }
//
//                    JSONArray paper_keywords = resultJSON.optJSONArray("paper_keywords");
//
//                    if (paper_keywords != null) {
//                        for (int i = 0; i < paper_keywords.length(); i++) {
//                            JSONObject keyword = paper_keywords.getJSONObject(i);
//                            String keywordName = keyword.getString("keyword");
//                            int mentions = keyword.getInt("mentions");
//                            keywordList.add(new TermMention(keywordName, mentions));
//
//                        }
//                    }
//
//                    JSONArray insights = resultJSON.optJSONArray("insights");
//
//                    if (insights != null) {
//                        for (int i = 0; i < insights.length(); i++) {
//                            JSONObject insight = insights.getJSONObject(i);
//                            String insightTxt = insight.getString("text");
//                            insightList.add(insightTxt);
//
//                        }
//                    }
//                    JSONArray terms = resultJSON.getJSONArray("terms");
//
//                    for (int i = 0; i < insights.length(); i++) {
//                        String term = terms.getString(i);
//                        termList.add(term);
//
//                    }
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
                            publicationList));
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
                    if (project.PublicationList != null) {
                        for (Publication pub : project.PublicationList) {

                            PreparedStatement selectstatement = connection.prepareStatement("Select * from publication where pubid=? OR referenceId=?");
                            selectstatement.setString(1, pub.pmcId);
                            selectstatement.setString(2, pub.pmcId);
                            ResultSet existedPubs = selectstatement.executeQuery();

                            if (!existedPubs.next()) {

                                bulkInsert = null;
                                insertSql = "INSERT INTO public.publication(\n"
                                        + "            pubid, referenceid, repository, title, fulltext, abstract, journalissn, \n"
                                        + "            doi, pubyear, keywords, batchid)    \n"
                                        + "    VALUES (?, ?, ?, ?, ?, ?, ?, \n"
                                        + "            ?, ?, ?, ?) \n";

                                bulkInsert = connection.prepareStatement(insertSql);
                                bulkInsert.setString(1, pub.pmcId);
                                bulkInsert.setString(2, pub.pmcId);
                                bulkInsert.setString(3, "CORDIS");
                                bulkInsert.setString(4, pub.Title);
                                bulkInsert.setString(5, "");
                                bulkInsert.setString(6, pub.Abstract);
                                bulkInsert.setString(7, "");
                                bulkInsert.setString(8, pub.DOI);
                                bulkInsert.setString(9, pub.Year);
                                bulkInsert.setString(10, "");
                                bulkInsert.setString(11, pub.Year);
                                bulkInsert.executeUpdate();

                                bulkInsert = null;
                                insertSql = "insert into pubproject (pubid, projectid) values (?,?)";
                                bulkInsert = connection.prepareStatement(insertSql);
                                bulkInsert.setString(1, pub.pmcId);
                                bulkInsert.setString(2, projectId);
                                bulkInsert.executeUpdate();

                                bulkInsert = null;
                                insertSql = "insert into pubkeyword (pubid, keyword) values (?,?)";
                                bulkInsert = connection.prepareStatement(insertSql);
                                if (pub.KeywordList != null) {
                                    for (String keyword : pub.KeywordList) {
                                        bulkInsert.setString(1, pub.pmcId);
                                        bulkInsert.setString(2, keyword);
                                        bulkInsert.executeUpdate();

                                    }
                                }

                                bulkInsert = null;
                                insertSql = "insert into pubmeshterm (pubid, descriptor, count) values (?,?, ?)";
                                bulkInsert = connection.prepareStatement(insertSql);
                                if (pub.MeSHTerms != null) {
                                    for (String meshTerm : pub.MeSHTerms) {
                                        bulkInsert.setString(1, pub.pmcId);
                                        bulkInsert.setString(2, meshTerm);
                                        bulkInsert.setInt(3, 1);
                                        bulkInsert.executeUpdate();

                                    }
                                }
                            }
                            /*
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
                             */

                        }
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
