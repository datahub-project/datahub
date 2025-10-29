import { Maybe } from 'graphql/jsutils/Maybe';
import { parseJsonArrayOrDefault, parseMaybeStringAsFloatOrDefault } from '@app/shared/numberUtil';

import {
    AssertionResult,
    AssertionStdParameter,
    AssertionStdParameterType,
    AssertionValueChangeType,
    StringMapEntry,
} from '@types';

/**
 * Gets the main metric on an assertion's results that are being monitored over time
 * @param runEvent
 * @returns {number | undefined}
 */
export const tryGetPrimaryMetricValueFromAssertionRunEvent = (): number | undefined => {
    return undefined;
};

// This captures context around the expected range of values
type AssertionRangeEndType = 'inclusive' | 'exclusive';
