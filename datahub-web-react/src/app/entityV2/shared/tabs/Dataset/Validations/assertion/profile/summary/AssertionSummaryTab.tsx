/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { AssertionSummaryContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';

import { Assertion } from '@types';

type Props = {
    loading: boolean;
    assertion?: Assertion;
};

export const AssertionSummaryTab = ({ loading, assertion }: Props) => {
    return (
        <>
            {loading || !assertion ? (
                <AssertionSummaryLoading />
            ) : (
                <AssertionSummaryContent assertion={assertion as Assertion} />
            )}
        </>
    );
};
