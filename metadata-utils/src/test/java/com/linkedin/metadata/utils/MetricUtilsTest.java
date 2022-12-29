package com.linkedin.metadata.utils;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests the capabilities of {@link MetricUtils}
 */

public class MetricUtilsTest {

    /**
     *  CodaHale VS Micrometer - Differences
     *  CodaHale counter can be decreased but Micrometer one can only be increased.
     *  Metrics is changed in LocalEbeanServerConfigFactory.
     */
    @Test
    public void testCounterMetric() {
        String counterMetric = "counterMetric";
        for (int i = 0; i < 3; i++) {
            MetricUtils.counterInc(counterMetric);
        }
        assertEquals(3, MetricUtils.getCodaHaleRegistry().counter(counterMetric).getCount());
        assertEquals(3.0, MetricUtils.getMicrometerRegistry().counter(counterMetric).count());
        MetricUtils.counterInc(2, counterMetric);
        assertEquals(5, MetricUtils.getCodaHaleRegistry().counter(counterMetric).getCount());
        assertEquals(5.0, MetricUtils.getMicrometerRegistry().counter(counterMetric).count());
    }

    /**
     *  CodaHale VS Micrometer - Differences
     *  CodaHale histogram rounds numbers and can provide only predefined percentile.
     *  On the other side, Micrometer provide more precise data and percentile are settable.
     */
    @Test
    public void testHistogramMetric() {
        String histogramMetric = "histogramMetric";
        for (int i = 0; i < 50; i++) {
            MetricUtils.updateHistogram(i, histogramMetric);
        }
        assertEquals(50, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getCount());
        assertEquals(37.0, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getSnapshot().get75thPercentile());
        assertEquals(47.0, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getSnapshot().get95thPercentile());
        assertEquals(48.0, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getSnapshot().get98thPercentile());
        assertEquals(49.0, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getSnapshot().get99thPercentile());
        assertEquals(49.0, MetricUtils.getCodaHaleRegistry().histogram(histogramMetric).getSnapshot().get999thPercentile());

        assertEquals(50, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().count());
        assertEquals(37.9375, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().percentileValues()[0].value());
        assertEquals(47.9375, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().percentileValues()[1].value());
        assertEquals(49.9375, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().percentileValues()[2].value());
        assertEquals(49.9375, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().percentileValues()[3].value());
        assertEquals(49.9375, MetricUtils.getMicrometerHistogram(histogramMetric).takeSnapshot().percentileValues()[4].value());
    }

    /**
     *  CodaHale VS Micrometer - Differences
     *  CodaHale uses Timer.Context to store the context and Micrometer is using Timer.Sample for the same purpose.
     *  In order to make the code independent of the Metric provide, the UUID is used in both cases.
     */
    @Test
    public void testTimerMetric() throws InterruptedException {
        String timerMetric = "timerMetric";
        for (int i = 0; i < 5; i++) {
            UUID timerSample = MetricUtils.timerStart(timerMetric);
            Thread.sleep(1000 + (i * 250));
            MetricUtils.timerStop(timerSample, timerMetric);
        }
        assertEquals(5, MetricUtils.getCodaHaleRegistry().timer(timerMetric).getSnapshot().size());
        assertTrue(Math.abs(MetricUtils.getCodaHaleRegistry().timer(timerMetric).getSnapshot().getMax() / 1000000 - 2000) < 100);
        assertTrue(Math.abs(MetricUtils.getCodaHaleRegistry().timer(timerMetric).getSnapshot().get75thPercentile() / 1000000 - 1750) < 100);
        assertTrue(Math.abs(MetricUtils.getCodaHaleRegistry().timer(timerMetric).getSnapshot().get95thPercentile() / 1000000 - 2000) < 100);
        assertTrue(Math.abs(MetricUtils.getCodaHaleRegistry().timer(timerMetric).getSnapshot().get99thPercentile() / 1000000 - 2000) < 100);

        assertEquals(5, MetricUtils.getMicrometerTimer(timerMetric).count());
        assertTrue(Math.abs(MetricUtils.getMicrometerTimer(timerMetric).max(TimeUnit.MILLISECONDS) - 2000) < 100);
        assertTrue(Math.abs(MetricUtils.getMicrometerTimer(timerMetric).takeSnapshot().percentileValues()[0].value(TimeUnit.MILLISECONDS) -  1750) < 100);
        assertTrue(Math.abs(MetricUtils.getMicrometerTimer(timerMetric).takeSnapshot().percentileValues()[1].value(TimeUnit.MILLISECONDS) -  2000) < 100);
        assertTrue(Math.abs(MetricUtils.getMicrometerTimer(timerMetric).takeSnapshot().percentileValues()[2].value(TimeUnit.MILLISECONDS) -  2000) < 100);
    }

    @Test
    public void testExceptionMetric() {
        Throwable throwable = new NullPointerException();
        String exceptionMetric = "exceptionMetric";
        MetricUtils.exceptionCounter(throwable, exceptionMetric);
        assertEquals(1, MetricUtils.getCodaHaleRegistry().counter(exceptionMetric).getCount());
        assertEquals(1.0, MetricUtils.getMicrometerRegistry().counter(exceptionMetric).count());
    }
}