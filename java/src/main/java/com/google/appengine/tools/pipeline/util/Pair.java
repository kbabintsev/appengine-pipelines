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

package com.google.appengine.tools.pipeline.util;

import java.io.Serializable;

/**
 * An ordered pair.
 *
 * @param <S> The type of the first element of the pair
 * @param <T> The type of the second element of the pair
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class Pair<S, T> implements Serializable {

    private static final long serialVersionUID = 579685806755133349L;

    private final S first;
    private final T second;

    public Pair(S first, T second) {
        this.first = first;
        this.second = second;
    }

    public static <S, T> Pair<S, T> of(S first, T second) {
        return new Pair<>(first, second);
    }

    public S getFirst() {
        return first;
    }

    public T getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return "Pair(" + first + ", " + second + ")";
    }
}
