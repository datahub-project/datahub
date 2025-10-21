import { Card, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { formatNumber } from '@app/shared/formatNumber';

import { Highlight as HighlightType } from '@types';

type Props = {
    highlight: HighlightType;
    shortenValue?: boolean;
};

const TitleText = styled(Text)`
    font-size: 16px;
    font-weight: 600;
    text-align: center;
    margin-bottom: 8px;
`;

const ValueText = styled(Text)`
    font-size: 36px;
    font-weight: 700;
    line-height: 1.2;
`;

const TrendText = styled(Text)`
    margin-top: 8px;
`;

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    padding: 16px 8px;
    gap: 4px;
`;

export const Highlight = ({ highlight, shortenValue }: Props) => {
    return (
        <Card title="">
            <ContentWrapper>
                <TitleText color="gray">{highlight.title}</TitleText>
                <ValueText color="violet">
                    {(shortenValue && formatNumber(highlight.value)) || highlight.value}
                </ValueText>
                {highlight.body && (
                    <TrendText color="gray" size="sm">
                        {highlight.body}
                    </TrendText>
                )}
            </ContentWrapper>
        </Card>
    );
};
