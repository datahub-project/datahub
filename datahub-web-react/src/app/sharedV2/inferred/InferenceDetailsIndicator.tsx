import React from 'react';
import styled from 'styled-components';
import { Popover } from '@components';
import { Sparkle } from 'phosphor-react';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const AISparkle = styled(Sparkle)`
    color: #a196e8;
    font-weight: bold;
    height: 14px;
    width: 14px;
    margin-right: 4px;

    &:hover {
        color: #4b39bc;
    }
`;

export default function InferenceDetailsIndicator() {
    const popoverContent = <PopoverWrapper>Generated with AI</PopoverWrapper>;

    return (
        <Popover content={popoverContent}>
            <AISparkle />
        </Popover>
    );
}
