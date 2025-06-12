import { ClockCircleOutlined } from '@ant-design/icons';
import { Button, Text } from '@components';
import { Divider, Typography } from 'antd';
import { Info } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { Tooltip } from '@components/components/Tooltip';
import colors from '@components/theme/foundations/colors';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getAnomalyFeedbackContext } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { PrimaryButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/PrimaryButton';
import { isExternalAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';
import {
    toReadableLocalDateTimeString,
    useAssertionFeedbackActions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import { ProviderSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/ProviderSummarySection';
import { AssertionResultPill } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/AssertionResultPill';
import {
    ResultStatusType,
    getDetailedErrorMessage,
    getFormattedActualVsExpectedTextForVolumeAssertion,
    getFormattedExpectedResultText,
    getFormattedReasonText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { useAppConfig } from '@app/useAppConfig';

import {
    Assertion,
    AssertionResultType,
    AssertionRunEvent,
    AssertionSourceType,
    Monitor,
    VolumeAssertionType,
} from '@types';

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

const ResultRow = styled.div``;

const ReasonRow = styled.div``;

const SecondaryHeader = styled.div`
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
`;

const ReasonText = styled.div``;

const ActualVsExpectedText = styled.div``;

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

const RESULT_TYPE_TO_COLOR = {
    Fail: colors.red[1000],
    Pass: colors.green[1000],
};

const ColoredActual = styled.span<{ status?: AssertionResultType }>`
    color: ${(props) => {
        switch (props.status) {
            case AssertionResultType.Failure:
                return RESULT_TYPE_TO_COLOR.Fail;
            case AssertionResultType.Success:
                return RESULT_TYPE_TO_COLOR.Pass;
            default:
                return 'inherit';
        }
    }};
`;

const VerticalDivider = styled.span`
    display: inline-block;
    width: 1px;
    height: 1em;
    background-color: ${colors.gray[100]};
    margin: 0 4px;
    vertical-align: middle;
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    run?: AssertionRunEvent;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
    resultStatusType?: ResultStatusType;
    refetchResults?: () => Promise<unknown>;
};

const RawValueTooltipTitle = ({ value }: { value?: string }) => {
    if (value === undefined) return null;
    return <div>Raw Value: {value}</div>;
};

// TODO: Add this in the assertion list, as hover on the timeline as well.
export const AssertionResultPopoverContent = ({
    assertion,
    monitor,
    run,
    showProfileButton,
    resultStatusType,
    onClickProfileButton,
    refetchResults,
}: Props) => {
    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;

    const runResultType = run?.result?.type;

    // Last run time
    const timestamp = run && new Date(run?.timestampMillis);
    const reportedTimestamp = run && run?.lastObservedMillis;

    // Result
    const actualVsExpectedText = run ? getFormattedActualVsExpectedTextForVolumeAssertion(run) : undefined;
    const hasActualVsExpectedText = !!actualVsExpectedText?.actualText;
    const isTypeVolumeAbsolute = run?.result?.assertion?.volumeAssertion?.type === VolumeAssertionType.RowCountTotal;
    const showResult = isTypeVolumeAbsolute && (hasActualVsExpectedText || runResultType === AssertionResultType.Error);

    // Reason
    const result = run?.result ? run.result! : undefined;
    const reasonText = run ? getFormattedReasonText(assertion, run) : undefined;
    const hasReason = !!reasonText;

    // Should show reason if it is either NOT a volume absolute assertion or if it is and there is an error
    const isTypeVolumeAbsoluteWithError = isTypeVolumeAbsolute && runResultType === AssertionResultType.Error;
    const showReason = hasReason && (!isTypeVolumeAbsolute || isTypeVolumeAbsoluteWithError);

    // Context
    const expectedText = run ? getFormattedExpectedResultText(assertion.info, run) : undefined;
    const hasContext = !!expectedText;

    // Error
    const errorMessage = (run && getDetailedErrorMessage(run)) || undefined;
    const hasDetailedError = runResultType === AssertionResultType.Error && !!errorMessage;

    // Platform
    const isExternal = isExternalAssertion(assertion);
    const hasPlatform = !!assertion.platform;

    // Smart assertion
    const isSmartAssertion = assertion.info?.source?.type === AssertionSourceType.Inferred;
    const { isFeedbackEnabled, isAnomaly, isMissedAlarm, isFalseAlarm, anomalyFeedbackCta } = getAnomalyFeedbackContext(
        assertion,
        run,
        onlineSmartAssertionsEnabled,
    );
    const showAnomalyFeedback = isFeedbackEnabled && resultStatusType !== ResultStatusType.LATEST;
    const showUndoFeedbackAction = isMissedAlarm || isFalseAlarm;

    const { isActionProcessing, onToggleAnomaly, onRetrainAsNewNormal, retrainModal } = useAssertionFeedbackActions({
        assertion,
        monitor,
        run,
        isAnomaly,
        refetchResults,
    });

    return (
        <>
            <HeaderRow>
                <TimestampContainer>
                    {/* NOTE: we don't show the assertion title in the header because the assertion's current title may not accurately represent the assertion that actually ran at this point in time. */}
                    <LastResultsRow>
                        {(timestamp && (
                            <>
                                <StyledClockCircleOutlined /> Ran{' '}
                                {toReadableLocalDateTimeString(run?.timestampMillis)}{' '}
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
                    <AssertionResultPill result={result} type={resultStatusType} isSmartAssertion={isSmartAssertion} />
                    {(showProfileButton && onClickProfileButton && (
                        <PrimaryButton title="Details" onClick={onClickProfileButton} />
                    )) ||
                        undefined}
                </Actions>
            </HeaderRow>
            {showResult && (
                <>
                    <ThinDivider />
                    <ResultRow>
                        <SecondaryHeader>Result</SecondaryHeader>
                        <ActualVsExpectedText>
                            {runResultType !== AssertionResultType.Error && (
                                <>
                                    Actual:{' '}
                                    <Text weight="bold" type="span">
                                        <ColoredActual status={runResultType}>
                                            {actualVsExpectedText?.actualText}
                                        </ColoredActual>
                                    </Text>
                                    <VerticalDivider />
                                </>
                            )}
                            {runResultType === AssertionResultType.Init ? (
                                <>Expected: Training...</>
                            ) : (
                                <>
                                    Expected:{' '}
                                    {actualVsExpectedText?.expectedLowText && (
                                        <Tooltip
                                            title={
                                                <RawValueTooltipTitle
                                                    value={actualVsExpectedText?.expectedLowTextWithDecimals}
                                                />
                                            }
                                        >
                                            <Text weight="bold" type="span">
                                                {actualVsExpectedText?.expectedLowText}
                                            </Text>
                                        </Tooltip>
                                    )}
                                    {actualVsExpectedText?.expectedLowText &&
                                        actualVsExpectedText?.expectedHighText &&
                                        ' - '}
                                    {actualVsExpectedText?.expectedHighText && (
                                        <Tooltip
                                            title={
                                                <RawValueTooltipTitle
                                                    value={actualVsExpectedText?.expectedHighTextWithDecimals}
                                                />
                                            }
                                        >
                                            <Text weight="bold" type="span">
                                                {actualVsExpectedText?.expectedHighText}
                                            </Text>
                                        </Tooltip>
                                    )}
                                </>
                            )}
                        </ActualVsExpectedText>
                    </ResultRow>
                </>
            )}
            {showReason && (
                <>
                    <ThinDivider />
                    <ReasonRow>
                        <SecondaryHeader>Reason</SecondaryHeader>
                        <ReasonText>{reasonText}</ReasonText>
                    </ReasonRow>
                </>
            )}
            {hasContext && !isTypeVolumeAbsolute && (
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
                              ...(result.externalUrl
                                  ? [
                                        <ThinDivider />,
                                        <PlatformRow>
                                            <a href={result.externalUrl} target="_blank" rel="noopener noreferrer">
                                                View results in{' '}
                                                {assertion.platform?.name &&
                                                assertion.platform?.name?.toLowerCase() !== 'unknown'
                                                    ? assertion.platform?.name
                                                    : 'source system.'}
                                            </a>
                                        </PlatformRow>,
                                    ]
                                  : []),
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

            {showAnomalyFeedback && (
                <>
                    <ThinDivider />
                    {showUndoFeedbackAction && (
                        <ContextRow>
                            <Typography.Paragraph style={{ color: RESULT_TYPE_TO_COLOR.Fail }}>
                                {result?.type === AssertionResultType.Success
                                    ? 'Marked as Anomaly.'
                                    : 'Marked as Normal.'}
                            </Typography.Paragraph>
                        </ContextRow>
                    )}
                    <ContextRow style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                        {!anomalyFeedbackCta.isInfo ? (
                            <Button
                                isLoading={isActionProcessing}
                                onClick={() => onToggleAnomaly(showUndoFeedbackAction)}
                                variant={showUndoFeedbackAction ? 'outline' : 'filled'}
                                color={showUndoFeedbackAction ? 'red' : 'primary'}
                            >
                                {showUndoFeedbackAction ? 'Undo' : anomalyFeedbackCta.message}
                            </Button>
                        ) : (
                            <Tooltip title={anomalyFeedbackCta.details}>
                                <Typography.Text type="secondary">
                                    <Info size={16} /> {anomalyFeedbackCta.message}
                                </Typography.Text>
                            </Tooltip>
                        )}
                        <Button variant="secondary" color="primary" onClick={onRetrainAsNewNormal}>
                            Train as New Normal
                        </Button>
                    </ContextRow>
                </>
            )}
            {retrainModal}
        </>
    );
};
