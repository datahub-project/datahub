import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const LoadingWrapper = styled.div<{ $marginTop?: number; $justifyContent: 'center' | 'flex-start' }>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    margin-top: ${(props) => (typeof props.$marginTop === 'number' ? `${props.$marginTop}px` : '25%')};
    width: 100%;
`;

const StyledLoading = styled(LoadingOutlined)<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
`;

interface Props {
    height?: number;
    marginTop?: number;
    justifyContent?: 'center' | 'flex-start';
}

export default function Loading({ height = 32, justifyContent = 'center', marginTop }: Props) {
    return (
        <LoadingWrapper $marginTop={marginTop} $justifyContent={justifyContent}>
            <StyledLoading $height={height} />
        </LoadingWrapper>
    );
}
