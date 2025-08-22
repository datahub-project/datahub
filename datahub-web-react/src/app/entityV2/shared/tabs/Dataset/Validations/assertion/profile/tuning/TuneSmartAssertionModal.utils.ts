import { MetricDataPoint } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';

import { ListMonitorMetricsQuery } from '@graphql/monitor.generated';
import { AnomalyReviewState } from '@types';

/**
 * Transform monitor metrics data into chart-ready format
 */
export function transformData(data: ListMonitorMetricsQuery | undefined): MetricDataPoint[] {
    if (!data?.listMonitorMetrics?.metrics) {
        return [];
    }

    const metrics: MetricDataPoint[] = [];

    data.listMonitorMetrics.metrics.forEach((metric) => {
        if (metric.assertionMetric?.timestampMillis && metric.assertionMetric?.value !== null) {
            const dataPoint: MetricDataPoint = {
                x: new Date(metric.assertionMetric.timestampMillis).toISOString(),
                y: metric.assertionMetric.value,
                hasAnomaly: !!metric.anomalyEvent && metric.anomalyEvent.state !== AnomalyReviewState.Rejected,
            };

            metrics.push(dataPoint);
        }
    });

    // Sort by timestamp
    return metrics.sort((a, b) => new Date(a.x).getTime() - new Date(b.x).getTime());
}
