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

import java.io.Serializable;

/**
 * A setting for specifying to the framework some aspect of a Job's execution.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface JobSetting extends Serializable {

    /**
     * A setting for specifying that a Job should not be run until the value slot
     * represented by the given {@code FutureValue} has been filled.
     */
    final class WaitForSetting implements JobSetting {

        private static final long serialVersionUID = 1961952679964049657L;
        private final Value<?> futureValue;

        public WaitForSetting(final Value<?> value) {
            futureValue = value;
        }

        public Value<?> getValue() {
            return futureValue;
        }
    }

    /**
     * An abstract parent object for integer settings.
     */
    abstract class IntValuedSetting implements JobSetting {

        private static final long serialVersionUID = -4853437803222515955L;
        private final int value;

        protected IntValuedSetting(final int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return getClass() + "[" + value + "]";
        }
    }

    /**
     * An abstract parent object for String settings.
     */
    abstract class StringValuedSetting implements JobSetting {

        private static final long serialVersionUID = 7756646651569386669L;
        private final String value;

        protected StringValuedSetting(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return getClass() + "[" + value + "]";
        }
    }

    /**
     * A setting for specifying how long to wait before retrying a failed job. The
     * wait time will be
     *
     * <pre>
     * {@code
     * backoffSeconds * backoffFactor ^ attemptNumber
     * }
     * </pre>
     */
    final class BackoffSeconds extends IntValuedSetting {

        public static final int DEFAULT = 15;
        private static final long serialVersionUID = -8900842071483349275L;

        public BackoffSeconds(final int seconds) {
            super(seconds);
        }
    }

    /**
     * A setting for specifying how long to wait before retrying a failed job. The
     * wait time will be
     *
     * <pre>
     * {@code
     * backoffSeconds * backoffFactor ^ attemptNumber
     * }
     * </pre>
     */
    final class BackoffFactor extends IntValuedSetting {

        public static final int DEFAULT = 2;
        private static final long serialVersionUID = 5879098639819720213L;

        public BackoffFactor(final int factor) {
            super(factor);
        }
    }

    /**
     * A setting for specifying how many times to retry a failed job.
     */
    final class MaxAttempts extends IntValuedSetting {

        public static final int DEFAULT = 3;
        private static final long serialVersionUID = 8389745591294068656L;

        public MaxAttempts(final int attempts) {
            super(attempts);
        }
    }

    /**
     * A setting for specifying which queue to run a job on.
     */
    final class OnQueue extends StringValuedSetting {

        public static final String DEFAULT = null;
        private static final long serialVersionUID = -5010485721032395432L;

        public OnQueue(final String queue) {
            super(queue);
        }
    }

    /**
     * A setting for specifying what routing (service, version, instance) to use
     */
    final class Routing implements JobSetting {

        public static final Route DEFAULT = null;
        private static final long serialVersionUID = -5609527860840931319L;
        private final Route route;

        public Routing(final Route route) {
            this.route = route;
        }

        public Route getRoute() {
            return route;
        }

        @Override
        public String toString() {
            return getClass() + "[" + route + "]";
        }
    }

    /**
     * A setting specifying the job's status console URL.
     */
    final class StatusConsoleUrl extends StringValuedSetting {

        private static final long serialVersionUID = -3079475300434663590L;

        public StatusConsoleUrl(final String statusConsoleUrl) {
            super(statusConsoleUrl);
        }
    }
}
