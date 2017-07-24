package org.madgik.utils;

import java.io.Serializable;
import cc.mallet.topics.TopicAssignment;


/**
 * This class combines a sequence of observed features with a sequence of hidden
 * "labels".
 */
public class MixTopicModelTopicAssignment implements Serializable {

    public String EntityId;
    public TopicAssignment[] Assignments; // <modality>, zero is always text

    public MixTopicModelTopicAssignment(String entityId, TopicAssignment[] assignments) {
        this.EntityId = entityId;
        this.Assignments = assignments;
    }

    public int hashCode() {
        return EntityId.hashCode();
    }

    public boolean equals(Object obj) {

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof MixTopicModelTopicAssignment)) {
            return false;
        }
        return (((MixTopicModelTopicAssignment) obj).EntityId.equals(this.EntityId));


    }
    /*public TopicAssignment (Instance instance, LabelSequence topicSequence,LabelSequence lblTopicSequence) {
     this.instance = instance;
     this.topicSequence = topicSequence;
     this.lblTopicSequence = lblTopicSequence;
     }*/
}
