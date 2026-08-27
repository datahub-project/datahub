import { Loader } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    width: 100%;
    height: 100%;
`;

interface Props {
    className?: string;
}

export function SuspenseBlock({ className }: Props) {
    return (
        <Wrapper className={className}>
            <Loader size="lg" />
        </Wrapper>
    );
}
