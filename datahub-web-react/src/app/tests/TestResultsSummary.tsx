import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Tag, Typography } from 'antd';
import { Tooltip } from '@components';
import RefreshIcon from '@mui/icons-material/Refresh';
import { SUCCESS_COLOR_HEX } from '../entity/shared/tabs/Incident/incidentUtils';
import { useGetTestResultsSummaryQuery } from '../../graphql/test.generated';
import { formatNumberWithoutAbbreviation } from '../shared/formatNumber';
import { NoResultsSummary } from './NoResultsSummary';
import { ANTD_GRAY } from '../entity/shared/constants';
import { PLACEHOLDER_TEST_URN } from './constants';
import TestResultsModal from './TestResultsModal';
import { TestResultType } from '../../types.generated';
import { toRelativeTimeString } from '../shared/time/timeUtils';

const Container = styled.div`
    padding: 4px;
    height: 80px;
    display: flex;
    flex-direction: column;
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
    color: ${ANTD_GRAY[8]};
    font-size: 10px;
    letter-spacing: 1px;
    margin-bottom: 12px;
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
    font-size: 8px;
    justify-content: space-between; // Adjust as needed to fit your layout
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: start; // Adjust this as needed
    gap: 10px; // Adjust the gap as needed
`;

const DEFAULT_MODAL_OPTIONS = { visible: false, defaultActive: TestResultType.Success };

type Props = {
    urn: string;
    name: string;
};

export const TestResultsSummary = ({ urn, name }: Props) => {
    const [resultsModalOptions, setResultsModalOptions] = useState(DEFAULT_MODAL_OPTIONS);

    const { data: results, refetch } = useGetTestResultsSummaryQuery({
        skip: !urn || urn === PLACEHOLDER_TEST_URN,
        variables: {
            urn,
        },
    });

    // Handler for the refresh button click
    const handleRefreshClick = () => {
        refetch(); // This will re-execute the query and update the component's state with the new data
    };

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

    const lastComputed =
        results?.test?.results?.lastRunTimestampMillis !== undefined
            ? toRelativeTimeString(results?.test?.results?.lastRunTimestampMillis || 0)
            : 'unknown';

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
            <LastComputed>
                Last computed: {lastComputed}
                <Tooltip title="Refresh summary results">
                    <RefreshIcon fontSize="small" onClick={handleRefreshClick} /> {/* Adjust fontSize as needed */}
                </Tooltip>
            </LastComputed>
        </Container>
    );
};
