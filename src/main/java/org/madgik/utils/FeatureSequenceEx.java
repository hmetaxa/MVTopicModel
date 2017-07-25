/* Copyright (C) 2002 Univ. of Massachusetts Amherst, Computer Science Dept.
 This file is part of "MALLET" (MAchine Learning for LanguagE Toolkit).
 http://www.cs.umass.edu/~mccallum/mallet
 This software is provided under the terms of the Common Public License,
 version 1.0, as published by http://www.opensource.org.  For further
 information, see the file `LICENSE' included with this distribution. */
package  org.madgik.utils;

import cc.mallet.types.Alphabet;

import cc.mallet.types.FeatureSequence;
import cc.mallet.types.Sequence;


/**
 * An implementation of {@link Sequence} that ensures that every Object in the
 * sequence has the same class. Feature sequences are mutable, and will expand
 * as new objects are added.
 *
 * @author Andrew McCallum
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
