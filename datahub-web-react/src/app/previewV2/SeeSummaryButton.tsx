import { FileTextOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

const SummaryButton = styled.div<{ maxWidth: number }>`
    align-items: center;
    border: 1px solid #328980;
    border-radius: 4px;
    background: #328980;
    color: white;
    display: flex;
    max-width: ${(props) => props.maxWidth}px;
    padding: 6px;
    position: relative;
    overflow: hidden;
    transition: max-width 0.4s ease-in-out;
    white-space: nowrap;

    :hover {
        cursor: pointer;
    }
`;

const SummaryButtonText = styled.span`
    font-size: 10px;
    font-weight: 600;
    margin-left: 8px;
`;

export default function SeeSummaryButton() {
    const [summaryButtonExpanded, setSummaryButtonExpanded] = useState(false);
    return (
        <SummaryButton
            onMouseEnter={() => setSummaryButtonExpanded(true)}
            onMouseLeave={() => setSummaryButtonExpanded(false)}
            maxWidth={summaryButtonExpanded ? 102 : 26}
        >
            <FileTextOutlined />
            <SummaryButtonText>View summary</SummaryButtonText>
        </SummaryButton>
    );
}
