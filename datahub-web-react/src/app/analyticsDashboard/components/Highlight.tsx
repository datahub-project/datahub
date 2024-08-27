import React from 'react';

import { Card, Typography } from 'antd';
import styled from 'styled-components';

import { Highlight as HighlightType } from '../../../types.generated';
import { formatNumber } from '../../shared/formatNumber';

type Props = {
    highlight: HighlightType;
    shortenValue?: boolean;
};

const HighlightCard = styled(Card)`
    width: 160px;
    height: 160px;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
    line-height: 0;
    margin: 10px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

const TitleText = styled(Typography.Text)`
    line-height: 1.4em;
    margin-top: -6px;
`;
const BodyText = styled(Typography.Text)`
    font-size: 8px;
    line-height: 1.4em;
`;

const TitleContainer = styled.div`
    margin-top: -8px;
`;

const BodyContainer = styled.div`
    margin-top: 8px;
`;

export const Highlight = ({ highlight, shortenValue }: Props) => {
    return (
        <HighlightCard>
            <Typography.Title level={1}>
                {(shortenValue && formatNumber(highlight.value)) || highlight.value}
            </Typography.Title>
            <TitleContainer>
                <TitleText strong>{highlight.title}</TitleText>
            </TitleContainer>
            <BodyContainer>
                <BodyText>{highlight.body}</BodyText>
            </BodyContainer>
        </HighlightCard>
    );
};
