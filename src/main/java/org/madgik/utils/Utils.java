/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.utils;

import static cc.mallet.types.MatrixOps.dotProduct;
import static cc.mallet.types.MatrixOps.twoNorm;
import org.knowceans.util.Vectors;

/**
 *
 * @author omiros
 */
public class Utils {

    public static double cosineSimilarity(double[] m1, double[] m2) {

        return dotProduct(m1, m2) / (twoNorm(m1) * twoNorm(m2));

    }

    double[] softmax(double[] xs) {

        double a = Double.POSITIVE_INFINITY; //-1000000000.0;
        for (int i = 0; i < xs.length; ++i) {
            if (xs[i] > a) {
                a = xs[i];
            }
        }

        double Z = 0.0;
        for (int i = 0; i < xs.length; ++i) {
            Z += Math.exp(xs[i] - a);
        }

        double[] ps = new double[xs.length];
        for (int i = 0; i < xs.length; ++i) {
            ps[i] = Math.exp(xs[i] - a) / Z;
        }

        return ps;
    }

}
