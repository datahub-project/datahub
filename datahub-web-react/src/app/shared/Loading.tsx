/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const LoadingWrapper = styled.div<{
    $marginTop?: number;
    $width?: number;
    $justifyContent: 'center' | 'flex-start';
    $alignItems: 'center' | 'flex-start' | 'none';
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
    margin-top: ${(props) => (typeof props.$marginTop === 'number' ? `${props.$marginTop}px` : '25%')};
    width: ${({ $width }) => ($width !== undefined ? `${$width}px` : '100%')};
`;

const StyledLoading = styled(LoadingOutlined)<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
`;

interface Props {
    height?: number;
    width?: number;
    marginTop?: number;
    justifyContent?: 'center' | 'flex-start';
    alignItems?: 'center' | 'flex-start' | 'none';
}

export default function Loading({
    height = 32,
    width,
    justifyContent = 'center',
    alignItems = 'none',
    marginTop,
}: Props) {
    return (
        <LoadingWrapper $marginTop={marginTop} $width={width} $justifyContent={justifyContent} $alignItems={alignItems}>
            <StyledLoading $height={height} />
        </LoadingWrapper>
    );
}
