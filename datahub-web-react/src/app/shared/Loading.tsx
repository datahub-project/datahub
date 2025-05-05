import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const LoadingWrapper = styled.div<{
    $marginTop?: number;
    $justifyContent: 'center' | 'flex-start';
    $alignItems: 'center' | 'flex-start' | 'none';
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
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
    alignItems?: 'center' | 'flex-start' | 'none';
}

export default function Loading({ height = 32, justifyContent = 'center', alignItems = 'none', marginTop }: Props) {
    return (
        <LoadingWrapper $marginTop={marginTop} $justifyContent={justifyContent} $alignItems={alignItems}>
            <StyledLoading $height={height} />
        </LoadingWrapper>
    );
}
