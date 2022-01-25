package com.linkedin.datahub.lineage.spark.model;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

public interface LineageConsumer extends Consumer<LineageEvent> {
}
