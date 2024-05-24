import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography } from 'antd';
import TrendingDownOutlinedIcon from '@mui/icons-material/TrendingDownOutlined';
import TrendingUpOutlinedIcon from '@mui/icons-material/TrendingUpOutlined';
import { Maybe } from 'graphql/jsutils/Maybe';
import { decimalToPercentStr } from '../../utils/statsUtil';
import { REDESIGN_COLORS } from '../../../../../constants';

interface TrendDetailProps {
    tooltipTitle: string;
    trendIcon?: JSX.Element;
    headline: string;
    proportion: Maybe<number> | undefined;
    change: number;
    showCount?: boolean; // indicate whether to show count or not
    count?: number; //  numerical stats count
}

const TrendDetailContainer = styled.div`
    display: flex;
    width: 150px;
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

const TrendingIconContainer = styled.div<{ color: string }>`
    border-radius: 50%;
    background: ${(props) => props.color};
    color: ${REDESIGN_COLORS.WHITE};
    height: 24px;
    width: 24px;
    padding: 4px;
    margin-top: 6px;
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

const TRENDING_UP_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        <TrendingUpOutlinedIcon style={{ fontSize: 16 }} />
    </TrendingIconContainer>
);

const TRENDING_DOWN_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.WARNING_RED}>
        <TrendingDownOutlinedIcon style={{ fontSize: 16 }} />
    </TrendingIconContainer>
);

const TrendDetail = ({
    tooltipTitle,
    trendIcon,
    headline,
    proportion,
    change,
    showCount = false,
    count = 0,
}: TrendDetailProps) => {
    let displayText;
    if (showCount && count > 0) {
        displayText = `${count} stats`;
    } else if (count < 1) {
        displayText = 'None';
    } else {
        displayText = decimalToPercentStr(proportion, 2);
    }
    return (
        <TrendDetailContainer>
            <Tooltip title={tooltipTitle}>
                {trendIcon}
                {change < 0 && TRENDING_DOWN_ICON}
                {change > 0 && TRENDING_UP_ICON}
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
