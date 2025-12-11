/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { AcrylAssertionDetailsHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionDetailsHeader';
import { AcrylAssertionResultsChart } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionResultsChart';

import { CronSchedule } from '@types';

const Container = styled.div`
    width: 100%;
    padding-left: 52px;
`;

type Props = {
    urn: string;
    schedule?: CronSchedule;
    lastEvaluatedAtMillis?: number | undefined;
    nextEvaluatedAtMillis?: number | undefined;
    isStopped?: boolean;
};

export const AcrylAssertionDetails = ({
    urn,
    schedule,
    lastEvaluatedAtMillis,
    nextEvaluatedAtMillis,
    isStopped = false,
}: Props) => {
    return (
        <Container>
            <AcrylAssertionDetailsHeader
                schedule={schedule}
                lastEvaluatedAtMillis={lastEvaluatedAtMillis}
                nextEvaluatedAtMillis={nextEvaluatedAtMillis}
                isStopped={isStopped}
            />
            <AcrylAssertionResultsChart urn={urn} />
        </Container>
    );
};
