import React from 'react';
import styled from 'styled-components';
import { Sparkle } from 'phosphor-react';
import { Tooltip } from '@components';

const AiSparkle = styled(Sparkle)`
    height: 16px;
    width: 16px;
`;

const Container = styled.div`
    span,
    path {
        color: #705ee4 !important;
    }
    color: #533fd1 !important;
    padding: 2px 6px 2px 4px;
    border-radius: 200px;
    border: 1px solid #ccebf6;
    background: linear-gradient(
        113deg,
        var(--gradient-5, #f1f3fd) 23.75%,
        var(--gradient-6, #e5e2f8) 66.64%,
        var(--gradient-7, #e5e2f8) 94.06%
    );
    display: inline-flex;
    align-items: center;
    gap: 4px;
    margin-right: 4px;
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
