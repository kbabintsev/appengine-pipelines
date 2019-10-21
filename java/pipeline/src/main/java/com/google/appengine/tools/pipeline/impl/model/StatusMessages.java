package com.google.appengine.tools.pipeline.impl.model;

import com.google.api.client.util.Lists;
import org.joda.time.DateTime;

import java.util.List;

public final class StatusMessages {
    private final int maxCount;
    private final int maxLength;
    private final List<String> statusMessages;

    public StatusMessages(final int maxCount, final int maxLength, final Iterable<String> statusMessages) {
        this.maxCount = maxCount;
        this.maxLength = maxLength;
        this.statusMessages = Lists.newArrayList(statusMessages);
    }

    public void add(final long attemptNumber, final String type, final String text) {
        final String line = "[" + attemptNumber + "] " + DateTime.now().toString() + " " + type + " " + text;
        statusMessages.add(line.length() > maxLength ? line.substring(0, maxLength - 3) + "..." : line);
    }

    public List<String> getAll() {
        List<String> messages = statusMessages;
        if (messages != null && messages.size() > maxCount) {
            messages = messages.subList(messages.size() - maxCount, messages.size());
            messages.set(0, "[Truncated]");
        }
        return messages;
    }
}
