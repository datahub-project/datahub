import { Maybe } from 'graphql/jsutils/Maybe';
import {
    Assertion,
    AssertionInfo,
    AssertionRunEvent,
    AssertionType,
    FieldAssertionInfo,
    FieldAssertionType,
} from '../../../../../../../../../../../types.generated';
import { AssertionDataPoint, AssertionResultChartData } from './charts/types';
import { getFieldMetricTypeReadableLabel } from '../../../../../fieldDescriptionUtils';
import { tryGetPrimaryMetricValueFromAssertionRunEvent } from '../../shared/resultExtractionUtils';

/**
 * Gets the Y value that we should be plotting on the graph from the assertion run event
 * @param runEvent
 * @returns {number | undefined}
 */
export const tryGetYValueForChartFromAssertionRunEvent = (): number | undefined => {
    return tryGetPrimaryMetricValueFromAssertionRunEvent();
};

/**
 * Get assertion data points that can be plotted on the various assertion charts
 * @param runEvents
 * @returns {AssertionDataPoint[]}
 */
export const getAssertionDataPointsFromRunEvents = (runEvents: AssertionRunEvent[]): AssertionDataPoint[] => {
    return (
        runEvents
            .filter((runEvent) => !!runEvent.result)
            // TODO(jayacryl): filter out run events that don't have the same general metrics as the latest run event
            // ie. if user changed a column assertion to do something completely different on a different column
            .map((runEvent) => {
                const result = runEvent.result!; // NOTE: we've done `!` because we filter it out in the earlier lambda
                const resultUrl = result.externalUrl;

                /**
                 * Create a "result" to render in the timeline chart.
                 */
                const dataPoint: AssertionDataPoint = {
                    time: runEvent.timestampMillis,
                    result: {
                        type: result.type,
                        resultUrl,
                        yValue: tryGetYValueForChartFromAssertionRunEvent(),
                    },
                    relatedRunEvent: runEvent,
                };
                return dataPoint;
            }) || []
    );
};

/**
 * Gets a Y axis label depending on the assertion type
 * @param assertionInfo
 * @returns {number | undefined}
 */
export const tryGetYAxisLabelForChartFromAssertionInfo = (
    assertionInfo?: AssertionInfo | Maybe<AssertionInfo>,
): string | undefined => {
    let label: string | undefined;
    switch (assertionInfo?.type) {
        case AssertionType.Volume:
            label = 'Row count';
            break;
        case AssertionType.Field:
            label = tryGetFieldAssertionYAxisLabel(assertionInfo.fieldAssertion);
            break;
        case AssertionType.Sql:
            label = 'SQL query result';
            break;
        case AssertionType.DataSchema:
            break;
        case AssertionType.Freshness:
            label = 'Freshness check results';
            break;
        case AssertionType.Dataset:
            break;
        default:
            break;
    }
    return label;
};

function tryGetFieldAssertionYAxisLabel(info?: Maybe<FieldAssertionInfo>): string | undefined {
    let label: string | undefined;
    switch (info?.type) {
        case FieldAssertionType.FieldValues:
            label = 'Invalid Rows';
            break;
        case FieldAssertionType.FieldMetric: {
            const maybeMetricType = info.fieldMetricAssertion?.metric;
            try {
                label = maybeMetricType && getFieldMetricTypeReadableLabel(maybeMetricType);
            } catch (e) {
                // Best attempt
            }
            label = label || maybeMetricType?.valueOf() || 'Metric Value';
            break;
        }
        default:
            break;
    }
    return label;
}

/**
 * Gets all the data necessary to plot assertion and its run events on viz charts
 * @param assertion
 * @param completedRuns
 * @returns {AssertionResultChartData}
 */
export const getAssertionResultChartData = (
    assertion: Assertion,
    completedRuns: AssertionRunEvent[],
): AssertionResultChartData => {
    const timelineDataPoints: AssertionDataPoint[] = getAssertionDataPointsFromRunEvents(completedRuns);
    const maybeYAxisLabel: string | undefined = tryGetYAxisLabelForChartFromAssertionInfo(assertion.info);
    return {
        dataPoints: timelineDataPoints,
        yAxisLabel: maybeYAxisLabel,
        context: {
            assertion,
        },
    };
};
