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

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

import java.util.UUID;

/**
 * An implementation of {@link FutureValue} that wraps a {@link Slot}.
 *
 * @param <E> The type of the value represented by this {@code FutureValue}
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class FutureValueImpl<E> implements FutureValue<E> {
    protected Slot slot;

    public FutureValueImpl(final Slot slt) {
        this.slot = slt;
    }

    public final Slot getSlot() {
        return slot;
    }

    @Override
    public UUID getSourceJobKey() {
        return slot.getSourceJobKey();
    }

    @Override
    public UUID getPipelineKey() {
        return slot.getPipelineKey();
    }

    @Override
    public String toString() {
        return "FutureValue[" + slot + "]";
    }
}
