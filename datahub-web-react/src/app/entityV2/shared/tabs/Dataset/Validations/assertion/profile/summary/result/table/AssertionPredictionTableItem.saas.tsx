import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getFormattedExpectedResultText } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { applyOpacityToHexColor } from '@app/shared/styleUtils';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { EmbeddedAssertion } from '@types';

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

const MAX_PREDICTIONS_TO_SHOW = 1;

type Props = {
    predictions: EmbeddedAssertion[];
};

export const AssertionPredictionTableItem = ({ predictions }: Props) => {
    const futurePredictionsInfo: {
        expectedText: string;
        timeLabel: string;
    }[] = predictions
        .map((prediction) => {
            const expectedText = getFormattedExpectedResultText(prediction.assertion);
            const timeLabel = prediction.evaluationTimeWindow?.startTimeMillis
                ? toRelativeTimeString(prediction.evaluationTimeWindow?.startTimeMillis)
                : 'Future';
            if (!expectedText || !timeLabel || !prediction.evaluationTimeWindow?.startTimeMillis) {
                return undefined;
            }
            return {
                expectedText,
                timeLabel,
                startTimeMillis: prediction.evaluationTimeWindow.startTimeMillis,
            };
        })
        .filter((exists) => !!exists)
        // type cast for typescript
        .map((el) => el as { expectedText: string; timeLabel: string; startTimeMillis: number })
        // sort ascending by startTimeMillis
        .sort((a, b) => a.startTimeMillis - b.startTimeMillis)
        .slice(0, MAX_PREDICTIONS_TO_SHOW);

    if (predictions.length === 0) {
        return null;
    }
    return (
        <Container highlightColor={applyOpacityToHexColor(colors.blue[500], 0.035)}>
            <Tooltip showArrow={false} placement="left" title="Next run">
                <TimeColumn>
                    <PreHeaderText>Predicted</PreHeaderText>
                    <HeaderText>Upcoming</HeaderText>
                </TimeColumn>
            </Tooltip>
            <ResultColumn>
                <PreHeaderText color={colors.blue[500]}>Future predictions</PreHeaderText>
                <HeaderText>
                    {futurePredictionsInfo.map((el) => (
                        <>
                            {el.expectedText} ({el.timeLabel})
                            <br />
                        </>
                    ))}
                    {/* Testing showing only latest */}
                    {/* {futurePredictions.length > MAX_PREDICTIONS_TO_SHOW ? (
                        <strong>{`+${futurePredictions.length - MAX_PREDICTIONS_TO_SHOW} more`}</strong>
                    ) : null} */}
                </HeaderText>
            </ResultColumn>
        </Container>
    );
};
