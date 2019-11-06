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
package org.apache.beam.runners.flink.translation.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;

public class CountingPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  private final Map<PValue, Integer> counts = new HashMap<>();

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    for (PValue value : node.getInputs().values()) {
      final Integer previous = counts.putIfAbsent(value, 1);
      if (previous != null) {
        counts.put(value, previous + 1);
      }
    }
  }

  public int getPValueCount(PValue value) {
    return counts.get(value);
  }
}
