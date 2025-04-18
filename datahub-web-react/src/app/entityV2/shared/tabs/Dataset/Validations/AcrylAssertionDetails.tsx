import React from 'react';
import styled from 'styled-components';
import { CronSchedule } from '../../../../../../types.generated';
import { AcrylAssertionDetailsHeader } from './AcrylAssertionDetailsHeader';
import { AcrylAssertionResultsChart } from './AcrylAssertionResultsChart';

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
