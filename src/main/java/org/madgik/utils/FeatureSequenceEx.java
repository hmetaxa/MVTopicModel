package  org.madgik.utils;
import cc.mallet.types.FeatureSequence;



/**
 * Extension of MALLET's FeatureSequence  *
 * @author Omiros Metaxas
 * <a href="mailto:mccallum@cs.umass.edu">mccallum@cs.umass.edu</a>
 */
public class FeatureSequenceEx {
    
    protected final FeatureSequence featureSequence;

    public FeatureSequenceEx(FeatureSequence featureSequence)
    {
        this.featureSequence = featureSequence;
    }
    

    public void setIndexAtPosition(int pos, int newIndex) {
        featureSequence.getFeatures()[pos] = newIndex;
        //features[pos] = newIndex;
    }
     
   
}
