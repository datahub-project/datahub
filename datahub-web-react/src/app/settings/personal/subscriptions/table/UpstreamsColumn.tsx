import React from 'react';
import { Switch, Typography } from 'antd';
import styled from 'styled-components/macro';

const UpstreamsColumnContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
`;

const UpstreamText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
`;

interface Props {
    record: any;
}

export function UpstreamsColumn({ record }: Props) {
    const isSubscribedToUpstreams: boolean = record.subscribedToUpstreams;
    const upstreamsCount: number = record.numUpstreams;

    return (
        <UpstreamsColumnContainer>
            <Switch size="small" checked={isSubscribedToUpstreams} />
            <UpstreamText>{upstreamsCount} upstream entities</UpstreamText>
        </UpstreamsColumnContainer>
    );
}
