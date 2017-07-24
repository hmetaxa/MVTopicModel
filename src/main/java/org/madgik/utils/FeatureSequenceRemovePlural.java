package org.madgik.utils;

import cc.mallet.pipe.Pipe;
import cc.mallet.types.*;

/**
 * Remove plural nouns from feature sequence, replacing existing idex
 * Does not replace existing alphabet
 */
public class FeatureSequenceRemovePlural extends Pipe {

    public FeatureSequenceRemovePlural() {
        super(new Alphabet(), null);

    }

    public FeatureSequenceRemovePlural(Alphabet dataAlphabet) {
        super(dataAlphabet, null);

    }

    private int findSingularIndexForTerm(String term) {
        Alphabet currentAlphabet = getDataAlphabet();
        String singular;
        int ret = -1;

        if (term.length() > 1) {
            singular = term.substring(0, term.length() - 1);
            ret = currentAlphabet.lookupIndex(singular);
            if (term.endsWith("s") && ret != -1) {
                return ret;
            }
        }

        if (term.length() > 2) {
            singular = term.substring(0, term.length() - 2);
            ret = currentAlphabet.lookupIndex(singular);
            if (term.endsWith("es") && ret != -1) {
                return ret;
            }
        }

        if (term.length() > 3) {
            singular = term.substring(0, term.length() - 3) + "y";
            ret = currentAlphabet.lookupIndex(singular);
            if (term.endsWith("ies") && ret != -1) {

                return ret;

            }

        }

        return -1;
    }

    public Instance pipe(Instance instance) {

        if (instance.getData() instanceof FeatureSequence) {

            FeatureSequence features = (FeatureSequence) instance.getData();

            for (int position = 0; position < features.size(); position++) {
                String tmp = (String) features.get(position);
                int newIndex = findSingularIndexForTerm(tmp);
                if (newIndex != -1) {
                    features.setIndexAtPosition(position, newIndex);
                }
                
            }

        } else {
            throw new IllegalArgumentException("Looking for a FeatureSequence, found a "
                    + instance.getData().getClass());
        }

        return instance;
    }

    static final long serialVersionUID = 1;

}
