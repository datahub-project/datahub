import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { Button, Tag, Typography } from 'antd';
import { SUCCESS_COLOR_HEX } from '../entity/shared/tabs/Incident/incidentUtils';
import { useGetTestResultsSummaryQuery } from '../../graphql/test.generated';
import { formatNumberWithoutAbbreviation } from '../shared/formatNumber';
import { NoResultsSummary } from './NoResultsSummary';
import { ANTD_GRAY } from '../entity/shared/constants';
import { PLACEHOLDER_TEST_URN } from './constants';
import TestResultsModal from './TestResultsModal';
import { TestResultType } from '../../types.generated';
import { toRelativeTimeString } from '../shared/time/timeUtils';
import Loading from '../shared/Loading';
import { colors } from '@src/alchemy-components';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between; // Helps push LastComputed to the bottom

    
`;

// const Container = styled.div`
//     display: flex;
//     flex-direction: column;
//     justify-content: space-between; // Helps push LastComputed to the bottom
//     height: 100%; // Ensure it has a height for margin-top: auto to work on LastComputed
// `;

const StyledButton = styled(Button)`
    margin: 0px;
    padding: 0px;
`;

const StyledTag = styled(Tag)`
    font-size: 12px;
`;

const Title = styled.div`
    color: ${colors.gray[600]}
    font-size: 10px;
`;

// Styled component for LastComputed section
// const LastComputed = styled.div`
//     align-self: flex-end; // Aligns to the right if Container uses display: flex
//     color: ${ANTD_GRAY[7]};
//     font-size: 8px;
//     margin-top: auto; // Pushes to the bottom if Container uses display: flex with flex-direction: column
// `;

const LastComputed = styled.div`
    display: flex;
    align-items: center;
    font-size: 10px;
    margin-top: 4px;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: start; // Adjust this as needed
    flex-wrap: wrap;
`;

const DEFAULT_MODAL_OPTIONS = { visible: false, defaultActive: TestResultType.Success };
const MAX_POLL_COUNT = 10;

type Props = {
    urn: string;
    name: string;
    editVersion: number;
};

export const TestResultsSummary = ({ urn, name, editVersion }: Props) => {
    const [resultsModalOptions, setResultsModalOptions] = useState(DEFAULT_MODAL_OPTIONS);

    const [isPolling, setIsPolling] = useState(false);

    const { data: results, refetch } = useGetTestResultsSummaryQuery({
        skip: !urn || urn === PLACEHOLDER_TEST_URN,
        variables: {
            urn,
        },
    });

    const hasResults = results?.test?.results?.passingCount || results?.test?.results?.failingCount;
    const passingCount =
        results?.test?.results?.passingCount !== undefined
            ? formatNumberWithoutAbbreviation(results?.test?.results?.passingCount)
            : '-';
    const failingCount =
        results?.test?.results?.failingCount !== undefined
            ? formatNumberWithoutAbbreviation(results?.test?.results?.failingCount)
            : '-';

    const testDefinitionMd5 =
        results?.test?.results?.testDefinitionMd5 !== undefined ? results?.test?.results?.testDefinitionMd5 : '-';

    // TODO: API should tell us when the next compute is
    const missingLastRunLabel = hasResults
        ? 'Next update within 24 hours.'
        : 'Pending compute. Update expected by tomorrow.';
    const lastComputedLabel = results?.test?.results?.lastRunTimestampMillis
        ? `Last computed: ${toRelativeTimeString(results.test.results.lastRunTimestampMillis)}`
        : missingLastRunLabel;

    // If card does not have results, or has recently been edited, refetch every 2-4s
    // TODO: Do this in the parent as a batch API call for all tests instead of getting it one test at a time...
    const pollTrackerRef = useRef({ count: 0, id: null as any });
    useEffect(() => {
        const pollTracker = pollTrackerRef.current;

        // If this test:
        // 1. has results
        // 2. and has not been edited
        // 3. and a previous poll didn't already exist for it (this happens if test is newly created)
        // then do nothing
        if (editVersion === 0 && hasResults && !pollTracker.count) {
            // make es lint happy
            return () => {};
        }

        const clearPoll = () => {
            clearInterval(pollTracker.id);
            setIsPolling(false);
        };

        const onPoll = () => {
            refetch();
            pollTracker.count++;
            // After a test has been edited, it may take a little while to full compute
            if (pollTracker.count === MAX_POLL_COUNT) {
                clearPoll();
                pollTracker.count = 0;
            }
        };

        // Start poll
        setIsPolling(true);
        // NOTE: we randomize between 2-4s so the test cards stagger their polling API calls
        pollTracker.id = setInterval(onPoll, 2000 + Math.random() * 2000);

        // Clean up interval on component unmount
        return clearPoll;
    }, [refetch, editVersion, hasResults]);

    return (
        <Container>
            <Title>RESULTS</Title>
            <ButtonContainer>
                {(hasResults && (
                    <>
                        <StyledButton
                            type="link"
                            onClick={() =>
                                setResultsModalOptions({ visible: true, defaultActive: TestResultType.Success })
                            }
                        >
                            <StyledTag>
                                <Typography.Text style={{ color: SUCCESS_COLOR_HEX }} strong>
                                    {passingCount}{' '}
                                </Typography.Text>
                                passing
                            </StyledTag>
                        </StyledButton>
                        <StyledButton
                            type="link"
                            onClick={() =>
                                setResultsModalOptions({ visible: true, defaultActive: TestResultType.Failure })
                            }
                        >
                            <StyledTag>
                                <Typography.Text style={{ color: 'red' }} strong>
                                    {failingCount}{' '}
                                </Typography.Text>
                                failing
                            </StyledTag>
                        </StyledButton>
                    </>
                )) || <NoResultsSummary />}
                {isPolling ? <Loading height={14} marginTop={0} alignItems="center" /> : null}
                {resultsModalOptions.visible && results && (
                    <TestResultsModal
                        urn={urn}
                        name={name}
                        testDefinitionMd5={testDefinitionMd5 || ''}
                        defaultActive={resultsModalOptions.defaultActive}
                        passingCount={passingCount}
                        failingCount={failingCount}
                        onClose={() => setResultsModalOptions(DEFAULT_MODAL_OPTIONS)}
                    />
                )}
            </ButtonContainer>
            <LastComputed>{lastComputedLabel}</LastComputed>
        </Container>
    );
};
