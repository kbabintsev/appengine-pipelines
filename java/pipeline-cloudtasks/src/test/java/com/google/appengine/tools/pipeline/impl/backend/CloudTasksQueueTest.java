package com.google.appengine.tools.pipeline.impl.backend;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import dev.failsafe.FailsafeException;
import junit.framework.TestCase;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public final class CloudTasksQueueTest extends TestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRertyHelperOk() {
        final AtomicInteger count = new AtomicInteger(0);
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                return null;
            });
        } catch (FailsafeException ex) {
            fail("Should not get here, caught " + ex);
        } catch (Exception ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(1, count.get());
    }

    public void testRertyHelperException() {
        final AtomicInteger count = new AtomicInteger(0);
        boolean caught = false;
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                throw new Exception();
            });
        } catch (FailsafeException ex) {
            assertEquals(Exception.class, ex.getCause().getClass());
            caught = true;
        } catch (Exception ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(1, count.get());
        assertTrue("Exception should be caught", caught);
    }

    public void testRertyHelperRuntimeException() {
        final AtomicInteger count = new AtomicInteger(0);
        boolean caught = false;
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                throw new RuntimeException();
            });
        } catch (FailsafeException ex) {
            fail("Should not get here, caught " + ex);
        } catch (RuntimeException ex) {
            assertEquals(RuntimeException.class, ex.getClass());
            caught = true;
        } catch (Exception ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(1, count.get());
        assertTrue("Exception should be caught", caught);
    }

    public void testRertyHelperGoogleJsonResponseException() {
        final AtomicInteger count = new AtomicInteger(0);
        boolean caught = false;
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                throw new GoogleJsonResponseException(
                        new HttpResponseException.Builder(500, "error", new HttpHeaders()),
                        new GoogleJsonError()
                );
            });
        } catch (FailsafeException ex) {
            assertEquals(GoogleJsonResponseException.class, ex.getCause().getClass());
            caught = true;
        } catch (Exception ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(1, count.get());
        assertTrue("Exception should be caught", caught);
    }

    public void testRertyHelperIOException() {
        final AtomicInteger count = new AtomicInteger(0);
        boolean caught = false;
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                throw new IOException();
            });
        } catch (FailsafeException ex) {
            assertEquals(IOException.class, ex.getCause().getClass());
            caught = true;
        } catch (Throwable ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(6, count.get());
        assertTrue("Exception should be caught", caught);
    }

    public void testRertyHelperSocketTimeoutException() {
        final AtomicInteger count = new AtomicInteger(0);
        boolean caught = false;
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                count.addAndGet(1);
                throw new SocketTimeoutException();
            });
        } catch (FailsafeException ex) {
            assertEquals(SocketTimeoutException.class, ex.getCause().getClass());
            caught = true;
        } catch (Throwable ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(6, count.get());
        assertTrue("Exception should be caught", caught);
    }

    public void testRertyHelperSocketTimeoutAndSuccess() {
        final AtomicInteger count = new AtomicInteger(0);
        try {
            CloudTasksQueue.RETRY_HELPER.get(() -> {
                if (count.get() == 3) {
                    return null;
                }
                count.addAndGet(1);
                throw new SocketTimeoutException();
            });
        } catch (Throwable ex) {
            fail("Should not get here, caught " + ex);
        }
        assertEquals(3, count.get());
    }
}
