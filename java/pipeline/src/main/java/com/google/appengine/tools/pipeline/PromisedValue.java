// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline;

import java.util.UUID;

/**
 * An abstract representation of a value slot that will be filled in when some
 * asynchronous external agent supplies a value.
 * <p>
 * An instance of {@code PromisedValue} is obtained from within the {@code
 * run()} method of a Job via the method {@link Job#newPromise()}. Once
 * obtained in this way, an instance of {@code PromisedValue} may be used in the
 * same way that any {@link FutureValue} may be used. The {@link #getKey()
 * key} of a promised value is an opaque identifier that uniquely represents
 * the value slot to the framework. This key should be passed to the external
 * agent who will use the key to supply the promised value via the method
 * {@link PipelineService#submitPromisedValue(UUID, UUID, Object)}. For example the
 * following code might appear inside of the {@code run()} method of a Job.
 * <blockquote>
 *
 * <pre>
 * PromisedValue<java.lang.Integer> x = newPromise()
 * UUID xKey = x.getKey();
 * invokeExternalAgent(xKey)
 * futureCall(new UsesIntegerJob(), x);
 * </pre>
 *
 * </blockquote> The external agent will provide the promised integer value at
 * some point in the future by invoking {@code
 * pipelineService.acceptPromisedValue(key, value)}, where {@code key} is
 * a String equal to {@code xKey} and {@code value} is some integer value.
 * The framework will then invoke the {@code run()} method of the {@code
 * UsesIntegerJob} passing in {@code value}.
 * Note that if the run method of the Job that created the promisedValue failed
 * attempts to {@code submitPromisedValue} will never succeed. A good practice
 * would be to pass the promisedValue to a child job before, optionally, passing
 * it externally.
 *
 * @param <E> The type of the value represented by this {@code PromisedValue}
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface PromisedValue<E> extends FutureValue<E> {
    UUID getKey();
}
