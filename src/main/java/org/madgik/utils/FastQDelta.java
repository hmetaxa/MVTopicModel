/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.utils;

/**
 *
 * @author hmetaxa
 */
public class FastQDelta {

    public int NewTopic;
    public int OldTopic;
    public int DocOldTopicCnt; //latest (current) value to update histograms
    public int DocNewTopicCnt; //latest (current) value to update histograms
    public int Type;
    public int Modality;

    public FastQDelta() {

    }

    public FastQDelta(int oldT, int newT, int type, int mod, int docOldTopicCnt, int docNewTopicCnt) {
        NewTopic = newT;
        OldTopic = oldT;
        Type = type;
        Modality = mod;
        DocOldTopicCnt = docOldTopicCnt;
        DocNewTopicCnt = docNewTopicCnt;
        

    }

}
