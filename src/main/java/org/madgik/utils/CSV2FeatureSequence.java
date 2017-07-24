package org.madgik.utils;

import cc.mallet.pipe.Pipe;
import java.io.*;
import java.util.ArrayList;

import cc.mallet.types.Alphabet;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.Instance;
import java.util.Arrays;

/**
 * Convert a list of strings into a feature sequence
 *
 * @ Target
 */
public class CSV2FeatureSequence extends Pipe {

    public long totalNanos = 0;
    String delimeter = "\\t";

    public CSV2FeatureSequence(Alphabet dataDict) {
        super(dataDict, null);
    }

    public CSV2FeatureSequence(Alphabet dataDict, String delimeter) {
        super(dataDict, null);
        this.delimeter = delimeter;
    }

    public CSV2FeatureSequence() {
        super(new Alphabet(), null);
    }

    public CSV2FeatureSequence(String delimeter) {
        super(new Alphabet(), null);
        this.delimeter = delimeter;
    }

    public Instance pipe(Instance carrier) {
        // if (!((String) carrier.getTarget()).isEmpty()) {
        long start = System.nanoTime();

        try {

            if (!((String) carrier.getData()).isEmpty()) {
                ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(((String) carrier.getData()).split(delimeter)));

                //String[] tokens = ((String)carrier.getTarget()).split("[ \\t]");
                FeatureSequence featureSequence
                        = new FeatureSequence((Alphabet) getAlphabet(), tokens.size());
                for (int i = 0; i < tokens.size(); i++) {
                    featureSequence.add(tokens.get(i));
                }

                carrier.setData(featureSequence);
            } else {
                FeatureSequence featureSequence
                        = new FeatureSequence((Alphabet) getAlphabet(), 0);
                carrier.setData(featureSequence);
            }

            totalNanos += System.nanoTime() - start;
        } catch (ClassCastException cce) {
            System.err.println("Expecting ArrayList<String>, found " + carrier.getData().getClass());
        }
        //}
        return carrier;
    }
    static final long serialVersionUID = 1;
}
