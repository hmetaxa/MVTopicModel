/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.dbpediaspotlightclient;

import java.util.List;
import java.util.Set;

/**
 *
 * @author omiros
 */
class DBpediaLink {

    public String label;
    public String uri;

    public DBpediaLink(String uri, String label) {
        this.label = label;
        this.uri = uri;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        DBpediaLink guest = (DBpediaLink) obj;
        return uri.equals(guest.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

}

public class DBpediaResource {

    private DBpediaLink link;
    private DBpediaResourceType type;
    //private String uri;
    private int support;
    private String mention;
    //private String title;
    private Set<DBpediaLink> abreviations;
    private double similarity;
    private double confidence;
    private String wikiAbstract;
    private String wikiId;
    private String meshId;
    private String mesh;
    private String icd10;
    private Set<DBpediaLink> categories;
    private Set<DBpediaLink> types;

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        DBpediaResource guest = (DBpediaResource) obj;
        return link.equals(guest.link);
    }

    @Override
    public int hashCode() {
        return link.hashCode();
    }

    public DBpediaResource(DBpediaResourceType type, String URI, String label, int support, double Similarity, double confidence,
            String mention, Set<DBpediaLink> categories, String wikiAbstract, String wikiId, Set<DBpediaLink> abreviations, Set<DBpediaLink> types, String meshId, String mesh, String icd10) {
        this.link = new DBpediaLink(URI, label);

        this.support = support;
        this.type = type;
        this.mention = mention;
        this.similarity = Similarity;
        this.confidence = confidence;

        this.categories = categories;
        this.abreviations = abreviations;
        this.wikiAbstract = wikiAbstract;
        this.wikiId = wikiId;
        this.mesh = mesh;
        this.meshId = meshId;
        this.icd10 = icd10;
        this.types = types;
    }

    public Set<DBpediaLink> getCategories() {
        return categories;
    }

    public void setCategories(Set<DBpediaLink> categories) {
        this.categories = categories;
    }

    public Set<DBpediaLink> getTypes() {
        return types;
    }

    public void setTypes(Set<DBpediaLink> categories) {
        this.types = categories;
    }

    public Set<DBpediaLink> getAbreviations() {
        return abreviations;
    }

    public void setAbreviations(Set<DBpediaLink> abreviations) {
        this.abreviations = abreviations;
    }

    public DBpediaLink getLink() {
        return link;
    }

    public void setURI(DBpediaLink Link) {
        this.link = Link;
    }

    public String getWikiId() {
        return wikiId;
    }

    public void setWikiId(String wikiId) {
        this.wikiId = wikiId;
    }

    public String getWikiAbstract() {
        return wikiAbstract;
    }

    public void setWikiAbstract(String wikiAbstract) {
        this.wikiAbstract = wikiAbstract;
    }

    public void setSimilarity(Double Similarity) {
        this.similarity = Similarity;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setConfidence(Double Confidence) {
        this.confidence = Confidence;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setMention(String Mention) {
        this.mention = Mention;
    }

    public String getMention() {
        return mention;
    }

    public void setType(DBpediaResourceType Type) {
        this.type = Type;
    }

    public DBpediaResourceType getType() {
        return type;
    }

    public int getSupport() {
        return support;
    }

    public void setSupport(int Support) {
        this.support = Support;
    }

    public String getMeshId() {
        return meshId;
    }

    public void setMeshId(String meshId) {
        this.meshId = meshId;
    }

    public String getMesh() {
        return mesh;
    }

    public void setMesh(String mesh) {
        this.mesh = mesh;
    }

    public String getIcd10() {
        return icd10;
    }

    public void setIcd10(String icd10) {
        this.icd10 = icd10;
    }

}
