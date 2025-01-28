import React from 'react';

import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

import { Assertion, AssertionResultType, AssertionRunEvent } from '../../../../../../../../../../types.generated';
import { AssertionResultPill } from '../../summary/shared/AssertionResultPill';
import { PrimaryButton } from '../../../builder/details/PrimaryButton';
import { isExternalAssertion } from '../isExternalAssertion';
import { ProviderSummarySection } from '../../summary/schedule/ProviderSummarySection';
import { ANTD_GRAY } from '../../../../../../../constants';
import { toReadableLocalDateTimeString } from '../utils';
import {
    ResultStatusType,
    getDetailedErrorMessage,
    getFormattedExpectedResultText,
    getFormattedReasonText,
} from '../../summary/shared/resultMessageUtils';

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
    color: ${ANTD_GRAY[7]};
    .anticon-clock-circle {
        font-size: 10px;
    }
`;

const ReasonRow = styled.div``;

const SecondaryHeader = styled.div`
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
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

const detailsButtonStyle = {
    borderRadius: 20,
    padding: '4px 12px',
};

type Props = {
    assertion: Assertion;
    run?: AssertionRunEvent;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
    resultStatusType?: ResultStatusType;
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
    const timestamp = run && new Date(run?.timestampMillis);
    const reportedTimestamp = run && run?.lastObservedMillis;

    // Reason
    const result = run?.result ? run.result! : undefined;
    const reasonText = run ? getFormattedReasonText(assertion, run) : undefined;
    const hasReason = !!reasonText;

    // Context
    const expectedText = run ? getFormattedExpectedResultText() : undefined;
    const hasContext = !!expectedText;

    // Error
    const errorMessage = (run && getDetailedErrorMessage(run)) || undefined;
    const hasDetailedError = run?.result?.type === AssertionResultType.Error && !!errorMessage;

    // Platform
    const isExternal = isExternalAssertion(assertion);
    const hasPlatform = !!assertion.platform;

    return (
        <>
            <HeaderRow>
                <TimestampContainer>
                    {/* NOTE: we don't show the assertion title in the header because the assertion's current title may not accurately represent the assertion that actually ran at this point in time. */}
                    <LastResultsRow>
                        {(timestamp && (
                            <>
                                <StyledClockCircleOutlined /> Ran {toReadableLocalDateTimeString(run?.timestampMillis)}{' '}
                            </>
                        )) || <>No results yet</>}
                    </LastResultsRow>
                    {reportedTimestamp && (
                        <LastRunRow>
                            Reported {reportedTimestamp && toReadableLocalDateTimeString(reportedTimestamp)}
                        </LastRunRow>
                    )}
                </TimestampContainer>
                <Actions>
                    <AssertionResultPill result={result} type={resultStatusType} />
                    {(showProfileButton && onClickProfileButton && (
                        <PrimaryButton title="Details" onClick={onClickProfileButton} style={detailsButtonStyle} />
                    )) ||
                        undefined}
                </Actions>
            </HeaderRow>
            {hasReason && (
                <>
                    <ThinDivider />
                    <ReasonRow>
                        <SecondaryHeader>Reason</SecondaryHeader>
                        <ReasonText>{reasonText}</ReasonText>
                    </ReasonRow>
                </>
            )}
            {hasContext && (
                <>
                    <ThinDivider />
                    <ContextRow>
                        <SecondaryHeader>Expected</SecondaryHeader>
                        <ExpectedText>{expectedText}</ExpectedText>
                    </ContextRow>
                </>
            )}
            {hasDetailedError && (
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
            )}
            {isExternal ? (
                <>
                    {/* Show the native results if it's an external platform, so the customers can see things like 'result' that they've emitted into DH */}
                    {result?.nativeResults?.length
                        ? [
                              <ThinDivider />,
                              <PlatformRow>
                                  {result.nativeResults.map((entry) => (
                                      <div>
                                          <Typography.Text strong>{entry.key}</Typography.Text>: {entry.value}
                                      </div>
                                  ))}
                              </PlatformRow>,
                          ]
                        : null}
                    {hasPlatform && (
                        <>
                            {/* Show the external platform details */}
                            <ThinDivider />
                            <PlatformRow>
                                <ProviderSummarySection assertion={assertion} showDivider={false} />
                            </PlatformRow>
                        </>
                    )}
                </>
            ) : null}
        </>
    );
};
