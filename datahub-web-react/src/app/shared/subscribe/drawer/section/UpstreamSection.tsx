import React from 'react';
import { Switch, Typography } from 'antd';
import styled from 'styled-components/macro';

const UpstreamContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
    display: grid;
    grid-template-columns: 1fr 15fr;
    column-gap: 8px;
    align-items: center;
`;

const UpstreamSwitch = styled(Switch)`
    grid-column: 1;
    grid-row: 1;
`;

const TitleText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
    grid-column: 2;
    grid-row: 1;
`;

const SubtitleText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    grid-column: 2;
    grid-row: 2;
`;

export default function UpstreamSection() {
    return (
        <>
            <UpstreamContainer>
                <UpstreamSwitch size="small" />
                <TitleText>Subscribe to changes for all upstream entities</TitleText>
                <SubtitleText>
                    There are currently 9 upstream entities, but this will change as upstream lineage changes.
                </SubtitleText>
            </UpstreamContainer>
        </>
    );
}
