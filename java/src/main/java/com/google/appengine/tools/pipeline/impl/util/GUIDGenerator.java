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

package com.google.appengine.tools.pipeline.impl.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * <p>
 * Test UUIDs:
 * <p>
 * |       | - always zeros for ease of identification
 * |  | - random number assigned statically to minimize clash with other developers
 * |       | - not used
 * |          | - incremental part
 * 00000000-0000-0000-0000-000000000000
 */
public final class GUIDGenerator {

    public static final String USE_SIMPLE_GUIDS_FOR_DEBUGGING =
            "com.google.appengine.api.pipeline.use-simple-guids-for-debugging";
    private static final String TEST_PREFIX = "00000000";
    private static AtomicInteger counter = new AtomicInteger();
    private static final int PART_0000 = 10000;
    private static int runId = (int) (Math.random() * PART_0000);

    private GUIDGenerator() {
    }

    public static String getTestPrefix() {
        return TEST_PREFIX + "-" + String.format("%04d", runId) + "-";
    }

    public static boolean isTest() {
        return Boolean.getBoolean(USE_SIMPLE_GUIDS_FOR_DEBUGGING);
    }

    public static synchronized UUID nextGUID() {
        if (Boolean.getBoolean(USE_SIMPLE_GUIDS_FOR_DEBUGGING)) {
            return UUID.fromString(getTestPrefix() + "0000-0000-" + String.format("%012d", counter.getAndIncrement()));
        }
        final UUID uuid = UUID.randomUUID();
        return uuid;
    }

    public static void main(final String[] args) {
//        for (int i = 0; i < 10; i++) {
//            System.out.println(nextGUID());
//        }
    }
}
