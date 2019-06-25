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

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

import java.util.UUID;

/**
 * Implements {@link PromisedValue} by wrapping a newly created {@link Slot}
 * which will contain the promised value when it is delivered.
 *
 * @param <E> The type of the value represented by this {@code PromisedValue}
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class PromisedValueImpl<E> extends FutureValueImpl<E> implements PromisedValue<E> {

    public PromisedValueImpl(final PipelineManager pipelineManager, final UUID pipelineKey, final UUID generatorJobKey, final UUID graphKey) {
        super(new Slot(pipelineManager, pipelineKey, generatorJobKey, graphKey));
    }

    @Override
    public UUID getKey() {
        return slot.getKey();
    }

}
