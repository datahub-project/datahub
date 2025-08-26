import { Popover } from '@components';
import { Sparkle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const AISparkle = styled(Sparkle)`
    color: #5c3fd1;
    font-weight: bold;
    height: 14px;
    width: 14px;
    margin-right: 4px;
    position: relative;
    top: 2px;

    &:hover {
        color: #4b39bc;
    }
`;

const Pill = styled.div`
    padding: 4px 12px;
    border-radius: 12px;
    background-color: #f3f3fa;
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
`;

export default function InferenceDetailsPill({ pillStyles }: { pillStyles?: React.CSSProperties }) {
    const popoverContent = (
        <PopoverWrapper>
            This description was auto-generated with
            <br />
            an AI inference model.
            <br />
            It may make mistakes. Check important info.
        </PopoverWrapper>
    );

    return (
        <Popover showArrow={false} content={popoverContent}>
            <Pill style={pillStyles}>
                <AISparkle />
                Generated with AI
            </Pill>
        </Popover>
    );
}
