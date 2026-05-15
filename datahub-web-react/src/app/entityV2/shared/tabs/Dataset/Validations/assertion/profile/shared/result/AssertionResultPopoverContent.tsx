import { ClockCircleOutlined } from '@ant-design/icons';
import { Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { PrimaryButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/PrimaryButton';
import { isExternalAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';
import { toReadableLocalDateTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import { ProviderSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/ProviderSummarySection';
import { AssertionResultPill } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/AssertionResultPill';
import { getAssertionResultSeverityDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionResultSeverityUtils';
import {
    ResultStatusType,
    getDetailedErrorMessage,
    getFormattedExpectedResultText,
    getFormattedReasonText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { Assertion, AssertionResult, AssertionResultType, AssertionRunEvent } from '@types';

const HeaderRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 4px;
`;

// NOTE: removed the title for now as the assertion's current title may not accurately describe the assertion that ran at this point in history
// const Title = styled.div`
//     flex: 1;
//     font-size: 16px;
//     font-weight: 600;
//     overflow: hidden;
//     > div {
//         overflow: hidden;
//         white-space: nowrap;
//         text-overflow: ellipsis;
//     }
//     margin-right: 20px;
// `;

const Actions = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
`;

const TimestampContainer = styled.div`
    gap: 5px;
    display: flex;
    flex-direction: column;
`;

const LastResultsRow = styled.div`
    font-size: 14px;
    display: flex;
    align-items: center;
    font-weight: 500;
`;
const LastRunRow = styled.div`
    font-size: 10px;
    display: flex;
    align-items: center;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textTertiary};
    .anticon-clock-circle {
        font-size: 10px;
    }
`;

const ReasonRow = styled.div``;

const SecondaryHeader = styled.div`
    font-size: 10px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const ReasonText = styled.div``;

const ContextRow = styled.div``;

const ExpectedText = styled.div``;

const PlatformRow = styled.div``;

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    margin-right: 4px;
    font-size: 12px;
`;

const ThinDivider = styled(Divider)`
    margin: 12px 0px;
    padding: 0px;
`;

type Props = {
    assertion: Assertion;
    run?: AssertionRunEvent;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
    resultStatusType?: ResultStatusType;
};

type PopoverHeaderProps = {
    timestamp?: Date;
    reportedTimestamp?: number;
    result?: AssertionResult;
    resultStatusType?: ResultStatusType;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
};

const PopoverHeader = ({
    timestamp,
    reportedTimestamp,
    result,
    resultStatusType,
    showProfileButton,
    onClickProfileButton,
}: PopoverHeaderProps) => {
    return (
        <HeaderRow>
            <TimestampContainer>
                {/* NOTE: we don't show the assertion title in the header because the assertion's current title may not accurately represent the assertion that actually ran at this point in time. */}
                <LastResultsRow>
                    {(timestamp && (
                        <>
                            <StyledClockCircleOutlined /> Ran {toReadableLocalDateTimeString(timestamp.getTime())}{' '}
                        </>
                    )) || <>No results yet</>}
                </LastResultsRow>
                {reportedTimestamp && (
                    <LastRunRow>Reported {toReadableLocalDateTimeString(reportedTimestamp)}</LastRunRow>
                )}
            </TimestampContainer>
            <Actions>
                <AssertionResultPill result={result} type={resultStatusType} />
                {(showProfileButton && onClickProfileButton && (
                    <PrimaryButton title="Details" onClick={onClickProfileButton} />
                )) ||
                    undefined}
            </Actions>
        </HeaderRow>
    );
};

const ReasonSection = ({ reasonText }: { reasonText?: string }) => {
    if (!reasonText) return null;
    return (
        <>
            <ThinDivider />
            <ReasonRow>
                <SecondaryHeader>Reason</SecondaryHeader>
                <ReasonText>{reasonText}</ReasonText>
            </ReasonRow>
        </>
    );
};

const ExpectedSection = ({ expectedText }: { expectedText?: string }) => {
    if (!expectedText) return null;
    return (
        <>
            <ThinDivider />
            <ContextRow>
                <SecondaryHeader>Expected</SecondaryHeader>
                <ExpectedText>{expectedText}</ExpectedText>
            </ContextRow>
        </>
    );
};

const SeveritySection = ({ result }: { result?: AssertionResult }) => {
    const severityDisplay = getAssertionResultSeverityDisplay(result);
    if (!severityDisplay) return null;

    return (
        <>
            <ThinDivider />
            <ContextRow>
                <SecondaryHeader>Severity</SecondaryHeader>
                <ExpectedText>{severityDisplay.label}</ExpectedText>
            </ContextRow>
        </>
    );
};

const MessageSection = ({ errorMessage, show }: { errorMessage?: string; show: boolean }) => {
    if (!show || !errorMessage) return null;
    return (
        <>
            <ThinDivider />
            <ContextRow>
                <SecondaryHeader>Message</SecondaryHeader>
                <Typography.Paragraph
                    ellipsis={{
                        expandable: true,
                        symbol: 'more',
                        rows: 3,
                        onExpand: (e) => e.stopPropagation(),
                    }}
                >
                    {errorMessage}
                </Typography.Paragraph>
            </ContextRow>
        </>
    );
};

const ExternalResultsSection = ({ assertion, result }: { assertion: Assertion; result?: AssertionResult }) => {
    if (!isExternalAssertion(assertion)) return null;

    const externalResultsSections: React.ReactNode[] = [];
    if (result?.nativeResults?.length) {
        externalResultsSections.push(
            <ThinDivider key="external-results-divider" />,
            <PlatformRow key="external-results">
                {result.nativeResults.map((entry) => (
                    <div key={entry.key}>
                        <Typography.Text strong>{entry.key}</Typography.Text>: {entry.value}
                    </div>
                ))}
            </PlatformRow>,
        );
        if (result.externalUrl) {
            externalResultsSections.push(
                <ThinDivider key="external-results-link-divider" />,
                <PlatformRow key="external-results-link">
                    <a href={result.externalUrl} target="_blank" rel="noopener noreferrer">
                        View results in{' '}
                        {assertion.platform?.name && assertion.platform?.name?.toLowerCase() !== 'unknown'
                            ? assertion.platform?.name
                            : 'source system.'}
                    </a>
                </PlatformRow>,
            );
        }
    }

    return (
        <>
            {/* Show the native results if it's an external platform, so customers can see source-emitted values. */}
            {externalResultsSections.length > 0 && externalResultsSections}
            {assertion.platform && (
                <>
                    <ThinDivider />
                    <PlatformRow>
                        <ProviderSummarySection assertion={assertion} showDivider={false} />
                    </PlatformRow>
                </>
            )}
        </>
    );
};

// TODO: Add this in the assertion list, as hover on the timeline as well.
export const AssertionResultPopoverContent = ({
    assertion,
    run,
    showProfileButton,
    resultStatusType,
    onClickProfileButton,
}: Props) => {
    // Last run time
    const timestamp = run?.timestampMillis ? new Date(run.timestampMillis) : undefined;
    const reportedTimestamp = run?.lastObservedMillis || undefined;

    // Reason
    const result = run?.result || undefined;
    const reasonText = run ? getFormattedReasonText(assertion, run) : undefined;
    const hasReason = !!reasonText;

    // Context
    const expectedText = run ? getFormattedExpectedResultText(assertion.info, run) : undefined;

    // Error
    const errorMessage = (run && getDetailedErrorMessage(run)) || undefined;
    const hasDetailedError = run?.result?.type === AssertionResultType.Error && !!errorMessage;

    return (
        <>
            <PopoverHeader
                timestamp={timestamp}
                reportedTimestamp={reportedTimestamp}
                result={result}
                resultStatusType={resultStatusType}
                showProfileButton={showProfileButton}
                onClickProfileButton={onClickProfileButton}
            />
            {hasReason && <ReasonSection reasonText={reasonText} />}
            <SeveritySection result={result} />
            <ExpectedSection expectedText={expectedText} />
            <MessageSection errorMessage={errorMessage} show={hasDetailedError} />
            <ExternalResultsSection assertion={assertion} result={result} />
        </>
    );
};
