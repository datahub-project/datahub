import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import { Maybe } from 'graphql/jsutils/Maybe';
import { decimalToPercentStr } from '../../utils/statsUtil';
import { REDESIGN_COLORS } from '../../../../../constants';

interface TrendDetailProps {
    tooltipTitle: string;
    headline: string;
    proportion: Maybe<number> | undefined;
    showCount?: boolean; // indicate whether to show count or not
    count?: number; //  numerical stats count
}

const TrendDetailContainer = styled.div`
    display: flex;
    margin-right: 40px;
    span {
        display: flex;
        gap: 5px;
        align-content: center;
        align-items: center;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const StatSummarySubtitle = styled.div`
    display: flex;
    flex-direction: row;
`;

const Headline = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 12px;
    font-weight: 700;
    line-height: 24px;
`;

const SubtitleText = styled(Typography.Text)<{ color?: string }>`
    color: ${(props) => (props.color ? props.color : REDESIGN_COLORS.DARK_GREY)};
    font-family: Mulish;
    font-size: 13px;
    line-height: 14px;
    font-weight: 400;
`;

const TrendDetail = ({ tooltipTitle, headline, proportion, showCount = false, count = 0 }: TrendDetailProps) => {
    let displayText;
    if (showCount && count > 0) {
        if (headline === 'Numerical stats') {
            displayText = `${count} stats`;
        } else {
            displayText = `${count} values`;
        }
    } else if (count < 1) {
        displayText = 'None';
    } else {
        displayText = decimalToPercentStr(proportion, 2);
    }
    return (
        <TrendDetailContainer>
            <Tooltip title={tooltipTitle}>
                <TitleContainer>
                    <Headline>{headline}</Headline>
                    <StatSummarySubtitle>
                        <SubtitleText color={showCount && count < 1 ? REDESIGN_COLORS.SECONDARY_LIGHT_GREY : undefined}>
                            {displayText}
                        </SubtitleText>
                    </StatSummarySubtitle>
                </TitleContainer>
            </Tooltip>
        </TrendDetailContainer>
    );
};

export default TrendDetail;
