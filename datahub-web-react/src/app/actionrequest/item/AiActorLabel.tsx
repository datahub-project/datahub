import React from 'react';
import styled from 'styled-components';
import { Sparkle } from 'phosphor-react';
import { Tooltip } from '@components';

const AiSparkle = styled(Sparkle)`
    height: 16px;
    width: 16px;
    margin-right: 4px;
`;

const Container = styled.span`
    span,
    path {
        color: #5c3fd1 !important;
    }
    color: #5c3fd1 !important;
`;

const AiName = styled.span`
    font-weight: 700;
`;

export default function AiActorLabel() {
    return (
        <Container>
            <Tooltip showArrow={false} title="Generated via DataHub AI">
                <AiSparkle />
                <AiName>DataHub AI</AiName>
            </Tooltip>
        </Container>
    );
}
