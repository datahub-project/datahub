import { Maybe } from "graphql/jsutils/Maybe";
import { Assertion, AssertionInfo, AssertionRunEvent, AssertionType, FieldAssertionType } from "../../../../../../../../../../../types.generated";
import { AssertionDataPoint, AssertionResultChartData } from "./charts/types";
import { getFieldMetricTypeReadableLabel } from "../../../../../fieldDescriptionUtils";
import { tryGetPrimaryMetricValueFromAssertionRunEvent } from "../../shared/resultUtils";


/**
 * Gets the Y value that we should be plotting on the graph from the assertion run event
 * @param runEvent 
 * @returns {number | undefined}
 */
export const tryGetYValueForChartFromAssertionRunEvent = (runEvent: AssertionRunEvent): number | undefined => {
    return tryGetPrimaryMetricValueFromAssertionRunEvent(runEvent)
}

/**
 * Get assertion data points that can be plotted on the various assertion charts
 * @param runEvents 
 * @returns {AssertionDataPoint[]}
 */
export const getAssertionDataPointsFromRunEvents = (runEvents: AssertionRunEvent[]): AssertionDataPoint[] => {
    return runEvents
        .filter((runEvent) => !!runEvent.result)
        // TODO(jayacryl): filter out run events that don't have the same general metrics as the latest run event
        // ie. if user changed a column assertion to do something completely different on a different column
        .map((runEvent) => {
            const { result } = runEvent;
            if (!result) throw new Error('Completed assertion run event does not have a result.');
            const resultUrl = result.externalUrl;

            /**
             * Create a "result" to render in the timeline chart.
             */
            const dataPoint: AssertionDataPoint = {
                time: runEvent.timestampMillis,
                result: {
                    type: result.type,
                    resultUrl,
                    yValue: tryGetYValueForChartFromAssertionRunEvent(runEvent),
                },
                relatedRunEvent: runEvent,
            };
            return dataPoint;
        }) || [];
}

/**
 * Gets a Y axis label depending on the assertion type
 * @param assertionInfo 
 * @returns {number | undefined}
 */
export const tryGetYAxisLabelForChartFromAssertionInfo = (assertionInfo?: AssertionInfo | Maybe<AssertionInfo>): string | undefined => {
    switch (assertionInfo?.type) {
        case AssertionType.Volume:
            return 'Row count';
        case AssertionType.Field:
            if (!assertionInfo.fieldAssertion?.type) {
                break;
            }
            // Handle field assertion types
            switch (assertionInfo.fieldAssertion.type) {
                case FieldAssertionType.FieldValues:
                    return 'Invalid Rows';
                case FieldAssertionType.FieldMetric: {
                    const maybeMetricType = assertionInfo.fieldAssertion.fieldMetricAssertion?.metric
                    try {
                        if (maybeMetricType) return getFieldMetricTypeReadableLabel(maybeMetricType)
                    } catch (e) {
                        // Best attempt
                    }
                    return maybeMetricType?.valueOf() || 'Metric Value';
                }
                default:
                    break;
            }
            break;
        case AssertionType.Sql:
            // TODO(jayacryl)
            break;
        case AssertionType.DataSchema:
            break;
        case AssertionType.Freshness:
            break;
        case AssertionType.Dataset:
            break;
        default:
            break;
    }
    return undefined;
}

/**
 * Gets all the data necessary to plot assertion and its run events on viz charts
 * @param assertion 
 * @param completedRuns 
 * @returns {AssertionResultChartData}
 */
export const getAssertionResultChartData = (assertion: Assertion, completedRuns: AssertionRunEvent[]): AssertionResultChartData => {
    const timelineDataPoints: AssertionDataPoint[] = getAssertionDataPointsFromRunEvents(completedRuns)
    const maybeYAxisLabel: string | undefined = tryGetYAxisLabelForChartFromAssertionInfo(assertion.info)
    return {
        dataPoints: timelineDataPoints,
        yAxisLabel: maybeYAxisLabel,
        context: {
            assertion,
        }
    }
}