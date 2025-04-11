import React from 'react';

import styled from 'styled-components';
import { Tooltip } from '@components';

import { Assertion, AssertionRunEvent } from '../../../../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../../../../../../../../shared/time/timeUtils';
import { getFormattedTimeString } from '../timeline/utils';
import { ResultStatusType, getFormattedReasonText, getFormattedResultText } from '../../shared/resultMessageUtils';
import { getResultColor } from '../../../../../assertionUtils';
import { AssertionResultPopover } from '../../../shared/result/AssertionResultPopover';
import { applyOpacityToHexColor } from '../../../../../../../../../../shared/styleUtils';

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
