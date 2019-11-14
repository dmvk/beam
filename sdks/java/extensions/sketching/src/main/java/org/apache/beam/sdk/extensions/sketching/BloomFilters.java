/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sketching;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.BloomFilter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnel;

public class BloomFilters {

  public static <T> GlobalBloomFilter<T> globally(
      Funnel<T> funnel, int expectedInsertions, double fpp) {
    return new GlobalBloomFilter<>(funnel, expectedInsertions, fpp);
  }

  private static class BloomFilterCoder<T> extends AtomicCoder<BloomFilter<T>> {

    private final Funnel<T> funnel;

    BloomFilterCoder(Funnel<T> funnel) {
      this.funnel = funnel;
    }

    @Override
    public void encode(BloomFilter<T> value, OutputStream outStream) throws IOException {
      value.writeTo(outStream);
    }

    @Override
    public BloomFilter<T> decode(InputStream inStream) throws IOException {
      return BloomFilter.readFrom(inStream, funnel);
    }
  }

  private static class BloomFilterCombiner<T>
      implements SerializableBiFunction<BloomFilter<T>, BloomFilter<T>, BloomFilter<T>> {

    @Override
    public BloomFilter<T> apply(BloomFilter<T> first, BloomFilter<T> second) {
      first.putAll(second);
      return first;
    }
  }

  private static class BloomFilterFn<T> extends DoFn<T, BloomFilter<T>> {

    private final Funnel<T> funnel;
    private final int expectedInsertions;
    private final double fpp;

    private BloomFilter<T> bloomFilter;

    BloomFilterFn(Funnel<T> funnel, int expectedInsertions, double fpp) {
      this.funnel = funnel;
      this.expectedInsertions = expectedInsertions;
      this.fpp = fpp;
    }

    @StartBundle
    public void startBundle() {
      bloomFilter = BloomFilter.create(funnel, expectedInsertions, fpp);
    }

    @ProcessElement
    public void processElement(@Element T element) {
      bloomFilter.put(element);
    }

    @FinishBundle
    public void finishBundle(OutputReceiver<BloomFilter<T>> outputReceiver) {
      outputReceiver.output(bloomFilter);
    }
  }

  private static class GlobalBloomFilter<T>
      extends PTransform<PCollection<T>, PCollectionView<BloomFilter<T>>> {

    private final Funnel<T> funnel;
    private final int expectedInsertions;
    private final double fpp;

    GlobalBloomFilter(Funnel<T> funnel, int expectedInsertions, double fpp) {
      this.funnel = funnel;
      this.expectedInsertions = expectedInsertions;
      this.fpp = fpp;
    }

    @Override
    public PCollectionView<BloomFilter<T>> expand(PCollection<T> input) {
      return input
          .apply(
              "CalculateBloomFilter",
              ParDo.of(new BloomFilterFn<>(funnel, expectedInsertions, fpp)))
          .setCoder(new BloomFilterCoder<>(funnel))
          .apply("CombineBloomFilters", Combine.globally(new BloomFilterCombiner<>()))
          .apply("CreateView", View.asSingleton());
    }
  }
}
