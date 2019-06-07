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

package com.google.appengine.tools.pipeline.impl.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * A container for holding the results of querying for all objects associated
 * with a given root Job.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class PipelineObjects {

    private static final Logger LOGGER = Logger.getLogger(PipelineObjects.class.getName());

    private JobRecord rootJob;
    private Map<UUID, JobRecord> jobs;
    private Map<UUID, Slot> slots;
    private Map<UUID, Barrier> barriers;
    private Map<UUID, JobInstanceRecord> jobInstanceRecords;

    /**
     * The {@code PipelineObjects} takes ownership of the objects passed in. The
     * caller should not hold references to them.
     */
    public PipelineObjects(final UUID rootJobKey, final Map<UUID, JobRecord> jobs, final Map<UUID, Slot> slots,
                           final Map<UUID, Barrier> barriers, final Map<UUID, JobInstanceRecord> jobInstanceRecords,
                           final Map<UUID, ExceptionRecord> failureRecords) {
        this.jobInstanceRecords = jobInstanceRecords;
        this.barriers = barriers;
        this.jobs = jobs;
        this.slots = slots;
        final Map<UUID, String> jobToChildGuid = new HashMap<>();
        for (final JobRecord job : jobs.values()) {
            jobToChildGuid.put(job.getKey(), job.getChildGraphGuid());
            if (job.getKey().equals(rootJobKey)) {
                this.rootJob = job;
            }
        }
        final Iterator<JobRecord> jobIterator = jobs.values().iterator();
        while (jobIterator.hasNext()) {
            final JobRecord job = jobIterator.next();
            if (job != rootJob) {
                final UUID parentKey = job.getGeneratorJobKey();
                final String graphGuid = job.getGraphGuid();
                if (parentKey == null || graphGuid == null) {
                    LOGGER.info("Ignoring a non root job with no parent or graphGuid -> " + job);
                    jobIterator.remove();
                } else if (!graphGuid.equals(jobToChildGuid.get(parentKey))) {
                    LOGGER.info("Ignoring an orphand job " + job + ", parent: " + jobs.get(parentKey));
                    jobIterator.remove();
                }
            }
        }
        if (null == rootJob) {
            throw new IllegalArgumentException(
                    "None of the jobs were the root job with key " + rootJobKey);
        }
        final Iterator<Slot> slotIterator = slots.values().iterator();
        while (slotIterator.hasNext()) {
            final Slot slot = slotIterator.next();
            final UUID parentKey = slot.getGeneratorJobKey();
            final String parentGuid = slot.getGraphGuid();
            if (parentKey == null && parentGuid == null
                    || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
                slot.inflate(barriers);
            } else {
                LOGGER.info("Ignoring an orphand slot " + slot + ", parent: " + jobs.get(parentKey));
                slotIterator.remove();
            }
        }
        final Iterator<Barrier> barriersIterator = barriers.values().iterator();
        while (barriersIterator.hasNext()) {
            final Barrier barrier = barriersIterator.next();
            final UUID parentKey = barrier.getGeneratorJobKey();
            final String parentGuid = barrier.getGraphGuid();
            if (parentKey == null && parentGuid == null
                    || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
                barrier.inflate(slots);
            } else {
                LOGGER.info("Ignoring an orphand Barrier " + barrier + ", parent: " + jobs.get(parentKey));
                barriersIterator.remove();
            }
        }
        for (final JobRecord jobRec : jobs.values()) {
            final Barrier runBarrier = barriers.get(jobRec.getRunBarrierKey());
            final Barrier finalizeBarrier = barriers.get(jobRec.getFinalizeBarrierKey());
            final Slot outputSlot = slots.get(jobRec.getOutputSlotKey());
            final JobInstanceRecord jobInstanceRecord = jobInstanceRecords.get(jobRec.getJobInstanceKey());
            ExceptionRecord failureRecord = null;
            final UUID failureKey = jobRec.getExceptionKey();
            if (null != failureKey) {
                failureRecord = failureRecords.get(failureKey);
            }
            jobRec.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
        }
    }

    public JobRecord getRootJob() {
        return rootJob;
    }

    public Map<UUID, JobRecord> getJobs() {
        return jobs;
    }

    public Map<UUID, Slot> getSlots() {
        return slots;
    }

    public Map<UUID, Barrier> getBarriers() {
        return barriers;
    }

    public Map<UUID, JobInstanceRecord> getJobInstanceRecords() {
        return jobInstanceRecords;
    }
}
