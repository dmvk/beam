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
package org.apache.beam.runners.flink.translation.functions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.flink.translation.utils.Workarounds;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Encapsulates a {@link DoFn} inside a Flink {@link RichMapPartitionFunction}.
 *
 * <p>We get a mapping from {@link TupleTag} to output index and must tag all outputs with the
 * output number. Afterwards a filter will filter out those elements that are not to be in a
 * specific output.
 */
public class SznFlinkDoFnFunction<InputT, OutputT>
    extends RichFlatMapFunction<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private final SerializablePipelineOptions serializedOptions;

  private final DoFn<InputT, OutputT> doFn;
  private final String stepName;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  private final WindowingStrategy<?, ?> windowingStrategy;

  private final Map<TupleTag<?>, Integer> outputMap;
  private final TupleTag<OutputT> mainOutputTag;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
  private transient DoFnRunner<InputT, OutputT> doFnRunner;
  private transient CollectorAware collectorAware;

  private boolean bundleStarted = false;

  public SznFlinkDoFnFunction(
      DoFn<InputT, OutputT> doFn,
      String stepName,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options,
      Map<TupleTag<?>, Integer> outputMap,
      TupleTag<OutputT> mainOutputTag,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoderMap,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {

    this.doFn = doFn;
    this.stepName = stepName;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputMap = outputMap;
    this.mainOutputTag = mainOutputTag;
    this.inputCoder = inputCoder;
    this.outputCoderMap = outputCoderMap;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public void flatMap(WindowedValue<InputT> value, Collector<WindowedValue<OutputT>> out) {
    if (!bundleStarted) {
      bundleStarted = true;
      doFnRunner.startBundle();
    }
    collectorAware.setCollector(out);
    doFnRunner.processElement(value);
  }

  @Override
  public void open(Configuration parameters) {
    // Note that the SerializablePipelineOptions already initialize FileSystems in the readObject()
    // deserialization method. However, this is a hack, and we want to properly initialize the
    // options where they are needed.
    FileSystems.setDefaultPipelineOptions(serializedOptions.get());
    doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn);

    // setup DoFnRunner
    final RuntimeContext runtimeContext = getRuntimeContext();
    final DoFnRunners.OutputManager outputManager;
    if (outputMap.size() == 1) {
      outputManager = new SznFlinkDoFnFunction.DoFnOutputManager();
    } else {
      // it has some additional outputs
      outputManager = new SznFlinkDoFnFunction.MultiDoFnOutputManager(outputMap);
    }

    final List<TupleTag<?>> additionalOutputTags = Lists.newArrayList(outputMap.keySet());

    DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            serializedOptions.get(),
            doFn,
            new FlinkSideInputReader(sideInputs, runtimeContext),
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            new FlinkNoOpStepContext(),
            inputCoder,
            outputCoderMap,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    if (!serializedOptions.get().as(FlinkPipelineOptions.class).getDisableMetrics()) {
      doFnRunner =
          new DoFnRunnerWithMetricsUpdate<>(
              stepName, doFnRunner, new FlinkMetricContainer(getRuntimeContext()));
    }

    this.collectorAware = (CollectorAware) outputManager;
    this.doFnRunner = doFnRunner;
  }

  @Override
  public void close() throws Exception {
    Exception suppressed = null;
    try {
      if (bundleStarted) {
        doFnRunner.finishBundle();
      }
    } catch (Exception e) {
      // Suppress exception, so we can properly teardown DoFn.
      suppressed = e;
    }
    try {
      Optional.ofNullable(doFnInvoker).ifPresent(DoFnInvoker::invokeTeardown);
      if (suppressed != null) {
        throw suppressed;
      }
    } finally {
      Workarounds.deleteStaticCaches();
    }
  }

  interface CollectorAware {

    void setCollector(Collector collector);
  }

  static class DoFnOutputManager implements DoFnRunners.OutputManager, CollectorAware {

    @Nullable private Collector collector;

    DoFnOutputManager() {}

    @Override
    public void setCollector(Collector collector) {
      this.collector = Objects.requireNonNull(collector);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      Objects.requireNonNull(collector)
          .collect(
              WindowedValue.of(
                  new RawUnionValue(0 /* single output */, output.getValue()),
                  output.getTimestamp(),
                  output.getWindows(),
                  output.getPane()));
    }
  }

  static class MultiDoFnOutputManager implements DoFnRunners.OutputManager, CollectorAware {

    @Nullable private Collector<WindowedValue<RawUnionValue>> collector;
    private Map<TupleTag<?>, Integer> outputMap;

    MultiDoFnOutputManager(Map<TupleTag<?>, Integer> outputMap) {
      this.outputMap = outputMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setCollector(Collector collector) {
      this.collector = Objects.requireNonNull(collector);
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      Objects.requireNonNull(collector)
          .collect(
              WindowedValue.of(
                  new RawUnionValue(outputMap.get(tag), output.getValue()),
                  output.getTimestamp(),
                  output.getWindows(),
                  output.getPane()));
    }
  }
}
