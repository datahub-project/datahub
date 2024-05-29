import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';

const Wrapper = styled(Typography.Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    color: inherit;
`;

export default function OverflowTitle({ title, className }: { title?: string; className?: string }) {
    return (
        <Wrapper className={className} ellipsis={{ tooltip: {} }}>
            {title}
        </Wrapper>
    );
}
