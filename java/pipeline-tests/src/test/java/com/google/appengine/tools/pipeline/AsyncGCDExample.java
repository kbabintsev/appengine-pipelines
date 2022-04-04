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

import com.google.appengine.tools.pipeline.demo.GCDExample;

import java.util.UUID;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class AsyncGCDExample {

    static volatile Callback callback;

    private AsyncGCDExample() {
    }

    /**
     * A Callback
     */
    public interface Callback {
        int getFirstInt();

        int getSecondInt();

        String getUserName();

        void acceptOutput(String output);
    }

    /**
     * 1. Start a new thread in which we ask the user for two integers. 2. After
     * the user responds, calculate the GCD of the two integers 3. Start a new
     * thread in which we ask the user for his name. 4. After the user responds,
     * print a message on the console with the results.
     */
    @SuppressWarnings("serial")
    public static class PrintGCDJob extends Job0<Void> {
        @Override
        public Value<Void> run() {
            final PromisedValue<Integer> a = newPromise();
            final PromisedValue<Integer> b = newPromise();
            asyncAskUserForTwoIntegers(a.getKey(), b.getKey());
            final FutureValue<Integer> gcd = futureCall(new GCDExample.GCDJob(), a, b);
            // Don't ask the user for his name until after he has already
            // answered the first prompt asking for two integers.
            final FutureValue<String> userName = futureCall(new AskUserForNameJob(), waitFor(b));
            futureCall(new PrintResultJob(), userName, a, b, gcd);
            return null;
        }

        private void asyncAskUserForTwoIntegers(final UUID aKey, final UUID bKey) {
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    final int a = callback.getFirstInt();
                    final int b = callback.getSecondInt();
                    try {
                        service.submitPromisedValue(getPipelineKey(), aKey, a);
                        service.submitPromisedValue(getPipelineKey(), bKey, b);
                    } catch (NoSuchObjectException e) {
                        throw new RuntimeException(e);
                    } catch (OrphanedObjectException e) {
                        return;
                    }
                }
            };
            thread.start();
        }
    }

    /**
     * Prints a prompt on the console asking the user for his name, and then
     * starts a new thread which waits for the user to enter his name.
     */
    @SuppressWarnings("serial")
    public static class AskUserForNameJob extends Job0<String> {
        @Override
        public Value<String> run() {
            final PromisedValue<String> userName = newPromise();
            asyncAskUserForName(userName.getKey());
            return userName;
        }

        private void asyncAskUserForName(final UUID key) {
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    final String name = callback.getUserName();
                    try {
                        service.submitPromisedValue(getPipelineKey(), key, name);
                    } catch (NoSuchObjectException e) {
                        throw new RuntimeException(e);
                    } catch (OrphanedObjectException e) {
                        return;
                    }
                }
            };
            thread.start();
        }
    }

    /**
     * Prints result message to console
     */
    @SuppressWarnings("serial")
    public static class PrintResultJob extends Job4<Void, String, Integer, Integer, Integer> {
        @Override
        public Value<Void> run(final String userName, final Integer a, final Integer b, final Integer gcd) {
            final String output =
                    "Hello, " + userName + ". The GCD of " + a + " and " + b + " is " + gcd + ".";
            callback.acceptOutput(output);
            return null;
        }
    }
}
