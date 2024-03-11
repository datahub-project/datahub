import React from 'react';

import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

import {
    Assertion,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
} from '../../../../../../../../../../types.generated';
import { AssertionDescription } from '../../summary/AssertionDescription';
import { AssertionResultPill } from '../../summary/shared/AssertionResultPill';
import { PrimaryButton } from '../../../builder/details/PrimaryButton';
import { isExternalAssertion } from '../isExternalAssertion';
import { ProviderSummarySection } from '../../summary/schedule/ProviderSummarySection';
import { ANTD_GRAY } from '../../../../../../../constants';
import { toReadableLocalDateTimeString } from '../timeUtils';
import { ResultStatusType, getDetailedErrorMessage, getFormattedReasonText } from '../../summary/shared/resultMessageUtils';

const HeaderRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: start;
    margin-bottom: 4px;
`;

const Title = styled.div`
    flex: 1;
    font-size: 16px;
    font-weight: 600;
    overflow: hidden;
    > div {
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
    margin-right: 20px;
`;

const Actions = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
`;

const LastResultsRow = styled.div`
    color: ${ANTD_GRAY[7]};
    display: flex;
    align-items: center;
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

    // Reason
    const result = run?.result as AssertionResult;
    const reasonText = (run && getFormattedReasonText(assertion, run)) || undefined;
    const hasReason = !!reasonText;

    // Context
    const hasContext = false;
    const expectedText = undefined; // TODO For us.

    // Error
    const errorMessage = (run && getDetailedErrorMessage(run)) || undefined;
    const hasDetailedError = run?.result?.type === AssertionResultType.Error && !!errorMessage;

    // Platform
    const isExternal = isExternalAssertion(assertion);
    const hasExternalPlatform = isExternal && assertion.platform;

    return (
        <>
            <HeaderRow>
                {/* NOTE: we don't show the assertion title in the header because the assertion's current title may not accurately represent the assertion that actually ran at this point in time. */}
                <Actions>
                    <AssertionResultPill result={result} type={resultStatusType} />
                    {(showProfileButton && onClickProfileButton && (
                        <PrimaryButton title="Details" onClick={onClickProfileButton} style={detailsButtonStyle} />
                    )) ||
                        undefined}
                </Actions>
            </HeaderRow>
            <LastResultsRow>
                {(timestamp && (
                    <>
                        <StyledClockCircleOutlined /> Evaluated {toReadableLocalDateTimeString(run?.timestampMillis)}{' '}
                    </>
                )) || <>No results yet</>}
            </LastResultsRow>
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
            {hasExternalPlatform && (
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
