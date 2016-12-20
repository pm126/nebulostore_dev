/*
 * Copyright 2008 ZXing authors
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
/*
 * This file was modified to suit it to the nebulostore project's checkstyle rules.
 */

package com.google.zxing.common.reedsolomon;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;

/**
 * <p>Implements Reed-Solomon enbcoding, as the name implies.</p>
 *
 * @author Sean Owen
 * @author William Rucklidge
 */
public final class ReedSolomonEncoder {

  private final GenericGF field_;
  private final List<GenericGFPoly> cachedGenerators_;

  @Inject
  public ReedSolomonEncoder(GenericGF field) {
    this.field_ = field;
    this.cachedGenerators_ = new ArrayList<>();
    cachedGenerators_.add(new GenericGFPoly(field, new int[]{1}));
  }

  private GenericGFPoly buildGenerator(int degree) {
    if (degree >= cachedGenerators_.size()) {
      GenericGFPoly lastGenerator = cachedGenerators_.get(cachedGenerators_.size() - 1);
      for (int d = cachedGenerators_.size(); d <= degree; d++) {
        GenericGFPoly nextGenerator = lastGenerator.multiply(
            new GenericGFPoly(field_, new int[] {1, field_.exp(d - 1 +
                field_.getGeneratorBase())}));
        cachedGenerators_.add(nextGenerator);
        lastGenerator = nextGenerator;
      }
    }
    return cachedGenerators_.get(degree);
  }

  public void encode(int[] toEncode, int ecBytes) {
    if (ecBytes == 0) {
      throw new IllegalArgumentException("No error correction bytes");
    }
    int dataBytes = toEncode.length - ecBytes;
    if (dataBytes <= 0) {
      throw new IllegalArgumentException("No data bytes provided");
    }
    GenericGFPoly generator = buildGenerator(ecBytes);
    int[] infoCoefficients = new int[dataBytes];
    System.arraycopy(toEncode, 0, infoCoefficients, 0, dataBytes);
    GenericGFPoly info = new GenericGFPoly(field_, infoCoefficients);
    info = info.multiplyByMonomial(ecBytes, 1);
    GenericGFPoly remainder = info.divide(generator)[1];
    int[] coefficients = remainder.getCoefficients();
    int numZeroCoefficients = ecBytes - coefficients.length;
    for (int i = 0; i < numZeroCoefficients; i++) {
      toEncode[dataBytes + i] = 0;
    }
    System.arraycopy(coefficients, 0, toEncode, dataBytes + numZeroCoefficients,
        coefficients.length);
  }

}
