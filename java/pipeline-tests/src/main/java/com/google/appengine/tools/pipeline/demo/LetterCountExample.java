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

package com.google.appengine.tools.pipeline.demo;

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A letter count pipeline job example.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class LetterCountExample {

    private LetterCountExample() {
    }

    public static SortedMap<Character, Integer> countLetters(final String text) {
        final SortedMap<Character, Integer> charMap = new TreeMap<>();
        for (final char c : text.toCharArray()) {
            incrementCount(c, 1, charMap);
        }
        return charMap;
    }

    private static void incrementCount(final char c, final int increment, final Map<Character, Integer> charMap) {
        final Integer countInteger = charMap.get(c);
        final int count = (null == countInteger ? 0 : countInteger) + increment;
        charMap.put(c, count);
    }

    public static void main(final String[] args) {
        final String text = "ab cd";
        final String regex = "[^a-z,A-Z]";
        final String[] words = text.split(regex);
        for (final String word : words) {
            System.out.println("[" + word + "]");
        }
    }

    /**
     * Letter counter job.
     */
    public static class LetterCounter extends Job1<SortedMap<Character, Integer>, String> {

        private static final long serialVersionUID = -42446767578960124L;

        @Override
        public Value<SortedMap<Character, Integer>> run(final String text) {
            addStatusMessage("Received text: " + text);
            final String[] words = text.split("[^a-zA-Z]");
            addStatusMessage("Split into " + words.length + " words");
            final List<FutureValue<SortedMap<Character, Integer>>> countsForEachWord = new LinkedList<>();
            for (final String word : words) {
                countsForEachWord.add(futureCall(new SingleWordCounterJob(), immediate(word)));
            }
            return futureCall(new CountCombinerJob(), futureList(countsForEachWord));
        }
    }

    /**
     * Character counter per word.
     */
    public static class SingleWordCounterJob extends Job1<SortedMap<Character, Integer>, String> {

        private static final long serialVersionUID = 3257449383642363412L;

        @Override
        public Value<SortedMap<Character, Integer>> run(final String word) {
            addStatusMessage("Received word: " + word);
            final SortedMap<Character, Integer> value = countLetters(word);
            addStatusMessage("Count result: " + value);
            return immediate(value);
        }
    }

    /**
     * Combiner for the letter counting.
     */
    public static class CountCombinerJob extends
            Job1<SortedMap<Character, Integer>, List<SortedMap<Character, Integer>>> {

        private static final long serialVersionUID = -142472702334430476L;

        @Override
        public Value<SortedMap<Character, Integer>> run(
                final List<SortedMap<Character, Integer>> listOfMaps) {
            addStatusMessage("Received " + listOfMaps.size() + " results");
            final SortedMap<Character, Integer> totalMap = new TreeMap<>();
            for (final SortedMap<Character, Integer> charMap : listOfMaps) {
                for (final Entry<Character, Integer> pair : charMap.entrySet()) {
                    incrementCount(pair.getKey(), pair.getValue(), totalMap);
                }
            }
            addStatusMessage("Total count: " + totalMap);
            return immediate(totalMap);
        }
    }
}
