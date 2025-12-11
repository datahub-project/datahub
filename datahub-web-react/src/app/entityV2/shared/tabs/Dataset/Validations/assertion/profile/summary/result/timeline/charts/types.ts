/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Maybe } from 'graphql/jsutils/Maybe';

import { Assertion, AssertionResultType, AssertionRunEvent } from '@types';

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
