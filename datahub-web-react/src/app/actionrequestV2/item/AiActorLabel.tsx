import { Tooltip } from '@components';
import { Sparkle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

const AiSparkle = styled(Sparkle)`
    height: 16px;
    width: 16px;
`;

const Container = styled.div`
    span,
    path {
        color: #705ee4 !important;
    }
    color: ${(props) => getColor('primary', 500, props.theme)} !important;
    padding: 2px 6px 2px 4px;
    border-radius: 200px;
    border: 1px solid #ccebf6;
    background: linear-gradient(113deg, #f1f3fd 23.75%, #e5e2f8 66.64%, #e5e2f8 94.06%);
    display: inline-flex;
    align-items: center;
    gap: 4px;
    line-height: normal;
`;

const AiName = styled.span`
    font-weight: 600;
`;

export default function AiActorLabel() {
    return (
        <Tooltip showArrow={false} title="Generated via DataHub AI">
            <Container>
                <AiSparkle weight="fill" />
                <AiName>AI</AiName>
            </Container>
        </Tooltip>
    );
}
