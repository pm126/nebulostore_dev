/*
 * Copyright 2007 ZXing authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zxing.common.reedsolomon;

import com.google.inject.Inject;


/**
 * <p>
 * Implements Reed-Solomon decoding, as the name implies.
 * </p>
 *
 * <p>
 * The algorithm will not be explained here, but the following references were helpful in creating
 * this implementation:
 * </p>
 *
 * <ul>
 * <li>Bruce Maggs.
 * <a href="http://www.cs.cmu.edu/afs/cs.cmu.edu/project/pscico-guyb/realworld/www/rs_decode.ps">
 * "Decoding Reed-Solomon Codes"</a> (see discussion of Forney's Formula)</li>
 * <li>J.I. Hall. <a href="www.mth.msu.edu/~jhall/classes/codenotes/GRS.pdf">
 * "Chapter 5. Generalized Reed-Solomon Codes"</a> (see discussion of Euclidean algorithm)</li>
 * </ul>
 *
 * <p>
 * Much credit is due to William Rucklidge since portions of this code are an indirect port of his
 * C++ Reed-Solomon implementation.
 * </p>
 *
 * @author Sean Owen
 * @author William Rucklidge
 * @author sanfordsquires
 */
public final class ReedSolomonDecoder {

  private final GenericGF field_;

  @Inject
  public ReedSolomonDecoder(GenericGF field) {
    this.field_ = field;
  }

  /**
   * <p>
   * Decodes given set of received codewords, which include both data and error-correction
   * codewords. Really, this means it uses Reed-Solomon to detect and correct errors, in-place, in
   * the input.
   * </p>
   *
   * Method was modified by Piotr Malicki by extracting a part of the code to a new method.
   *
   * @param received
   *          data and error-correction codewords
   * @param twoS
   *          number of error-correction codewords available
   * @throws ReedSolomonException
   *           if decoding fails for any reason
   */
  public void decode(int[] received, int twoS) throws ReedSolomonException {
    GenericGFPoly syndrome = calcSyndrome(received, twoS);
    if (syndrome == null) {
      return;
    }

    GenericGFPoly[] sigmaOmega = runEuclideanAlgorithm(field_.buildMonomial(twoS, 1), syndrome,
        twoS);
    GenericGFPoly sigma = sigmaOmega[0];
    GenericGFPoly omega = sigmaOmega[1];
    int[] errorLocations = findErrorLocations(sigma);
    int[] errorMagnitudes = findErrorMagnitudes(omega, errorLocations);
    for (int i = 0; i < errorLocations.length; i++) {
      int position = received.length - 1 - field_.log(errorLocations[i]);
      if (position < 0) {
        throw new ReedSolomonException("Bad error location");
      }
      received[position] = GenericGF.addOrSubtract(received[position], errorMagnitudes[i]);
    }
  }

  /**
   * Method added by Piotr Malicki, not included in the original implementation. Decoding based on
   * erasure information.
   *
   * This method is based mainly on the original decode method.
   *
   * @throws ReedSolomonException
   * @author Piotr Malicki
   */
  public void decode(int[] received, int twoS, int[] errorLocations) throws ReedSolomonException {
    GenericGFPoly syndrome = calcSyndrome(received, twoS);
    if (syndrome == null) {
      return;
    }

    int[] tmp = new int[errorLocations.length];
    for (int i = 0; i < errorLocations.length; i++) {
      tmp[i] = field_.exp(received.length - 1 - errorLocations[i]);
    }
    int[] sigmaCoeffs = new int[errorLocations.length + 1];
    sigmaCoeffs[errorLocations.length] = 1;
    for (int i = tmp.length - 1; i >= 0; i--) {
      for (int j = i; j < tmp.length; j++) {
        sigmaCoeffs[j] = GenericGF.addOrSubtract(sigmaCoeffs[j],
            field_.multiply(tmp[i], sigmaCoeffs[j + 1]));
      }
    }
    GenericGFPoly sigma = new GenericGFPoly(field_, sigmaCoeffs);
    GenericGFPoly omega = sigma.multiply(syndrome).divide(field_.buildMonomial(twoS, 1))[1];

    int[] errorMagnitudes = findErrorMagnitudes(omega, tmp);
    for (int i = 0; i < errorLocations.length; i++) {
      int position = errorLocations[i];
      if (position < 0) {
        throw new ReedSolomonException("Bad error location");
      }
      received[position] = GenericGF.addOrSubtract(received[position], errorMagnitudes[i]);
    }
  }

  private GenericGFPoly[] runEuclideanAlgorithm(GenericGFPoly first, GenericGFPoly second,
      int bigR) throws ReedSolomonException {
    // Assume a's degree is >= b's
    GenericGFPoly a = first;
    GenericGFPoly b = second;
    if (a.getDegree() < b.getDegree()) {
      GenericGFPoly temp = a;
      a = b;
      b = temp;
    }

    GenericGFPoly rLast = a;
    GenericGFPoly r = b;
    GenericGFPoly tLast = field_.getZero();
    GenericGFPoly t = field_.getOne();

    // Run Euclidean algorithm until r's degree is less than R/2
    while (r.getDegree() >= bigR / 2) {
      GenericGFPoly rLastLast = rLast;
      GenericGFPoly tLastLast = tLast;
      rLast = r;
      tLast = t;

      // Divide rLastLast by rLast, with quotient in q and remainder in r
      if (rLast.isZero()) {
        // Oops, Euclidean algorithm already terminated?
        throw new ReedSolomonException("r_{i-1} was zero");
      }
      r = rLastLast;
      GenericGFPoly q = field_.getZero();
      int denominatorLeadingTerm = rLast.getCoefficient(rLast.getDegree());
      int dltInverse = field_.inverse(denominatorLeadingTerm);
      while (r.getDegree() >= rLast.getDegree() && !r.isZero()) {
        int degreeDiff = r.getDegree() - rLast.getDegree();
        int scale = field_.multiply(r.getCoefficient(r.getDegree()), dltInverse);
        q = q.addOrSubtract(field_.buildMonomial(degreeDiff, scale));
        r = r.addOrSubtract(rLast.multiplyByMonomial(degreeDiff, scale));
      }

      t = q.multiply(tLast).addOrSubtract(tLastLast);

      if (r.getDegree() >= rLast.getDegree()) {
        throw new IllegalStateException("Division algorithm failed to reduce polynomial?");
      }
    }

    int sigmaTildeAtZero = t.getCoefficient(0);
    if (sigmaTildeAtZero == 0) {
      throw new ReedSolomonException("sigmaTilde(0) was zero");
    }

    int inverse = field_.inverse(sigmaTildeAtZero);
    GenericGFPoly sigma = t.multiply(inverse);
    GenericGFPoly omega = r.multiply(inverse);
    return new GenericGFPoly[] {sigma, omega};
  }

  private int[] findErrorLocations(GenericGFPoly errorLocator) throws ReedSolomonException {
    // This is a direct application of Chien's search
    int numErrors = errorLocator.getDegree();
    if (numErrors == 1) {
      // shortcut
      return new int[] {errorLocator.getCoefficient(1)};
    }
    int[] result = new int[numErrors];
    int e = 0;
    for (int i = 1; i < field_.getSize() && e < numErrors; i++) {
      if (errorLocator.evaluateAt(i) == 0) {
        result[e] = field_.inverse(i);
        e++;
      }
    }
    if (e != numErrors) {
      throw new ReedSolomonException("Error locator degree does not match number of roots");
    }
    return result;
  }

  private int[] findErrorMagnitudes(GenericGFPoly errorEvaluator, int[] errorLocations) {
    // This is directly applying Forney's Formula
    int s = errorLocations.length;
    int[] result = new int[s];
    for (int i = 0; i < s; i++) {
      int xiInverse = field_.inverse(errorLocations[i]);
      int denominator = 1;
      for (int j = 0; j < s; j++) {
        if (i != j) {
          // denominator = field.multiply(denominator,
          // GenericGF.addOrSubtract(1, field.multiply(errorLocations[j], xiInverse)));
          // Above should work but fails on some Apple and Linux JDKs due to a Hotspot bug.
          // Below is a funny-looking workaround from Steven Parkes
          int term = field_.multiply(errorLocations[j], xiInverse);
          int termPlus1 = (term & 0x1) == 0 ? term | 1 : term & ~1;
          denominator = field_.multiply(denominator, termPlus1);
        }
      }
      result[i] = field_.multiply(errorEvaluator.evaluateAt(xiInverse),
          field_.inverse(denominator));
      if (field_.getGeneratorBase() != 0) {
        result[i] = field_.multiply(result[i], xiInverse);
      }
    }
    return result;
  }

  /**
   * Method extracted by Piotr Malicki from the original decode method to simplify code.
   *
   * @return
   * @author Piotr Malicki
   */
  private GenericGFPoly calcSyndrome(int[] received, int twoS) {
    GenericGFPoly poly = new GenericGFPoly(field_, received);
    int[] syndromeCoefficients = new int[twoS];
    boolean noError = true;
    for (int i = 0; i < twoS; i++) {
      int eval = poly.evaluateAt(field_.exp(i + field_.getGeneratorBase()));
      syndromeCoefficients[syndromeCoefficients.length - 1 - i] = eval;
      if (eval != 0) {
        noError = false;
      }
    }
    if (noError) {
      return null;
    }

    return new GenericGFPoly(field_, syndromeCoefficients);
  }

}
