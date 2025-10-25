import { colors } from '@components';
import React from 'react';
import styled, { keyframes } from 'styled-components';

const bounce = keyframes`
    0%, 80%, 100% {
        transform: translateY(0);
    }
    40% {
        transform: translateY(-2px);
    }
`;

const IndicatorContainer = styled.div`
    display: flex;
    padding: 4px 24px 16px 24px;
    justify-content: flex-start;
`;

const IndicatorContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    background-color: ${colors.gray[100]};
    padding: 10px 14px;
    border-radius: 8px;
`;

const Dot = styled.span<{ delay: number }>`
    width: 6px;
    height: 6px;
    background-color: ${colors.gray[500]};
    border-radius: 50%;
    display: inline-block;
    animation: ${bounce} 1.4s infinite ease-in-out;
    animation-delay: ${(props) => props.delay}s;
`;

export const TypingIndicator: React.FC = () => {
    return (
        <IndicatorContainer>
            <IndicatorContent>
                <Dot delay={0} />
                <Dot delay={0.2} />
                <Dot delay={0.4} />
            </IndicatorContent>
        </IndicatorContainer>
    );
};
