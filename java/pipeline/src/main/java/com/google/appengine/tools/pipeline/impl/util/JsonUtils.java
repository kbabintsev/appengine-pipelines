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

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class JsonUtils {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private JsonUtils() {
    }

    // All code below is only for manually testing this class.

    public static String mapToJson(final Map<?, ?> map) {
        try {
            return JSON_FACTORY.toPrettyString(map); //new JSONObject(map)).toString(2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> fromJson(final String json) {
        try {
            return JSON_FACTORY.fromString(json, GenericJson.class);
        } catch (Exception e) {
            throw new RuntimeException("json=" + json, e);
        }
    }

    @Nullable
    public static String serialize(@Nullable final Object value) throws IOException {
        if (value == null) {
            return null;
        }
        return JSON_FACTORY.toString(value);
    }

    @Nullable
    public static <T> T deserialize(@Nullable final String value, final Class<T> destinationClass) throws IOException {
        if (value == null) {
            return null;
        }
        return JSON_FACTORY.fromString(value, destinationClass);
    }

    public static void main(final String[] args) throws Exception {
        /*
        final JSONObject x = new JSONObject();
        x.put("first", 5);
        x.put("second", 7);
        debugPrint("hello");
        debugPrint(7);
        debugPrint(3.14159);
        debugPrint("");
        debugPrint('x');
        debugPrint(x);
        debugPrint(null);

        final Map<String, Integer> map = new HashMap<>();
        map.put("first", 5);
        map.put("second", 7);
        debugPrint(map);

        final int[] array = new int[]{5, 7};
        debugPrint(array);

        final ArrayList<Integer> arrayList = new ArrayList<>(2);
        arrayList.add(5);
        arrayList.add(7);
        debugPrint(arrayList);

        final Collection<Integer> collection = new HashSet<>(2);
        collection.add(5);
        collection.add(7);
        debugPrint(collection);

        final Object object = new Object();
        debugPrint(object);

        final Map<String, String> map1 = new HashMap<>();
        map1.put("a", "hello");
        map1.put("b", "goodbye");

        final Object[] array2 = new Object[]{17, "yes", "no", map1};

        final Map<String, Object> map2 = new HashMap<>();
        map2.put("first", 5.4);
        map2.put("second", array2);
        map2.put("third", map1);

        debugPrint(map2);

        class MyBean {
            @SuppressWarnings("unused")
            public int getX() {
                return 11;
            }

            @SuppressWarnings("unused")
            public boolean isHot() {
                return true;
            }

            @SuppressWarnings("unused")
            public String getName() {
                return "yellow";
            }
        }

        debugPrint(new MyBean());
        */
    }
}
