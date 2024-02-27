import { AssertionRunEvent, AssertionType } from "../../../../../../../../../../../types.generated";
import { AssertionDataPoint } from "./types";

export const getAssertionDataPointsFromRunEvents = (runEvents: AssertionRunEvent[]): AssertionDataPoint[] => {
    return runEvents
        .filter((runEvent) => !!runEvent.result)
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
                    yValue: getYValueForChartFromAssertionRunEvent(runEvent),
                },
                relatedRunEvent: runEvent,
            };
            return dataPoint;
        }) || [];
}

/**
 * Gets the Y value that we should be plotting on the graph from the assertion run event
 * @param runEvent 
 * @returns {number | undefined}
 */
export const getYValueForChartFromAssertionRunEvent = (runEvent: AssertionRunEvent): number | undefined => {
    switch (runEvent.result?.assertion?.type) {
        case AssertionType.Sql:
            return (runEvent.result.actualAggValue?.valueOf());
        case AssertionType.Volume:
            return (runEvent.result.rowCount?.valueOf());
        case AssertionType.Field:
            return (runEvent.result.missingCount?.valueOf());
        case AssertionType.Dataset:
            return (runEvent.result.actualAggValue?.valueOf());
        case AssertionType.DataSchema:
            break;
        case AssertionType.Freshness:
            break;
        default:
            break;
    }
}
