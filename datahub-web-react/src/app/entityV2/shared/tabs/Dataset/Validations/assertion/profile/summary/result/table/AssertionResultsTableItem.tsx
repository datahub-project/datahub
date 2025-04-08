import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { AssertionResultPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { getFormattedTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import {
    ResultStatusType,
    getFormattedReasonText,
    getFormattedResultText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { getResultColor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { applyOpacityToHexColor } from '@app/shared/styleUtils';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';

import { Assertion, AssertionRunEvent } from '@types';

const Container = styled.div<{ highlightColor?: string }>`
    display: flex;
    align-items: start;
    justify-content: start;
    padding: 4px 8px;
    border-radius: 4px;
    :hover {
        background-color: ${(props) => props.highlightColor || ANTD_GRAY[2]};
    }
`;

const TimeColumn = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    align-items: start;
    width: 112px;
`;

const ResultColumn = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    align-items: start;
    flex: 1;
`;

const PreHeaderText = styled.div<{ color?: string }>`
    font-size: 12px;
    color: ${(props) => props.color || ANTD_GRAY[7]};
`;

const HeaderText = styled.div`
    font-size: 14px;
`;

type Props = {
    assertion: Assertion;
    run: AssertionRunEvent;
};

export const AssertionResultsTableItem = ({ assertion, run }: Props) => {
    const assertionRunTime = run.timestampMillis;
    const absoluteRunTime = getFormattedTimeString(assertionRunTime);
    const resultText = getFormattedResultText(run.result?.type);
    const resultTextColor = getResultColor(run.result?.type) || undefined;
    const reasonText = getFormattedReasonText(assertion, run);
    const highlightColor = getResultColor(run.result?.type);
    return (
        <Container
            highlightColor={(highlightColor && applyOpacityToHexColor(highlightColor, 0.035)) || resultTextColor}
        >
            <Tooltip showArrow={false} placement="left" title={toLocalDateTimeString(assertionRunTime)}>
                <TimeColumn>
                    <PreHeaderText>{toRelativeTimeString(assertionRunTime)}</PreHeaderText>
                    <HeaderText>{absoluteRunTime}</HeaderText>
                </TimeColumn>
            </Tooltip>
            <AssertionResultPopover
                assertion={assertion}
                run={run}
                showProfileButton={false}
                resultStatusType={ResultStatusType.LATEST}
                placement="bottom"
            >
                <ResultColumn>
                    <PreHeaderText color={resultTextColor}>{resultText}</PreHeaderText>
                    <HeaderText>{reasonText}</HeaderText>
                </ResultColumn>
            </AssertionResultPopover>
        </Container>
    );
};
