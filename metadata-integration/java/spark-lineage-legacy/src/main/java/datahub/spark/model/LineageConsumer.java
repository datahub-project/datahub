package datahub.spark.model;

import java.io.Closeable;
import java.util.function.Consumer;

public interface LineageConsumer extends Consumer<LineageEvent>, Closeable {}
