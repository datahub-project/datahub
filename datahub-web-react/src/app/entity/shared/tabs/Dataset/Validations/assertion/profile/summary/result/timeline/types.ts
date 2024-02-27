import { Maybe } from "graphql/jsutils/Maybe";
import { AssertionResultType, AssertionRunEvent, AssertionStdOperator, DateInterval } from "../../../../../../../../../../../types.generated";

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

export type TimeRange = {
    startMs: number;
    endMs: number;
};
