import { Maybe } from 'graphql/jsutils/Maybe';
import { Assertion, AssertionResultType, AssertionRunEvent } from '../../../../../../../../../../../../types.generated';

export type AssertionResult = {
    type: AssertionResultType;
    resultUrl?: Maybe<string>;
    yValue?: number; // for checks with varying y-values
};

export type AssertionDataPoint = {
    time: number;
    result: AssertionResult;
    relatedRunEvent: AssertionRunEvent;
};

export type AssertionResultChartData = {
    dataPoints: AssertionDataPoint[];
    yAxisLabel?: string;
    context: {
        assertion: Assertion;
    };
};

export type TimeRange = {
    startMs: number;
    endMs: number;
};

export enum AssertionChartType {
    ValuesOverTime,
    StatusOverTime,
    Freshness,
}
