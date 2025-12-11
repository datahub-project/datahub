/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AssertionSourceType, AssertionType } from '@types';

/**
 * Returns true if the assertion is definitely an external assertion,
 * meaning that was reported using a custom source or a well-supported integration
 * like dbt test or Great Expectations.
 */
export const isExternalAssertion = (assertion) => {
    return (
        assertion?.info?.type === AssertionType.Dataset || // Legacy, non-native assertion type.
        assertion?.info?.type === AssertionType.Custom ||
        (assertion?.info?.source?.type !== AssertionSourceType.Inferred &&
            assertion?.info?.source?.type !== AssertionSourceType.Native)
    );
};
