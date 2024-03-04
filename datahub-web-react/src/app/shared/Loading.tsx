import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 25%;
    width: 100%;
`;

const StyledLoading = styled(LoadingOutlined)<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
`;

interface Props {
    height?: number;
}

export default function Loading({ height = 32 }: Props) {
    return (
        <LoadingWrapper>
            <StyledLoading $height={height} />
        </LoadingWrapper>
    );
}
