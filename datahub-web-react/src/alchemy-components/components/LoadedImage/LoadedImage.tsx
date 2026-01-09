import { Skeleton } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';
import type { LoadedImageProps } from '@src/alchemy-components/components/LoadedImage/types';

const ImageContainer = styled.div<{ width?: string }>`
    width: ${(props) => props.width || 'auto'};
    margin: auto;
    padding-bottom: 2em;
`;

const StyledImage = styled.img<{ width?: string }>`
    width: ${(props) => props.width || 'auto'};
    height: auto;
    border-radius: 8px;
    margin: auto;
    padding-bottom: 2em;
`;

const ImageSkeleton = styled(Skeleton.Image)<{ width?: string }>`
    &&& {
        width: ${(props) => props.width || 'auto'};
        height: auto;

        .ant-skeleton-image {
            width: ${(props) => props.width || 'auto'};
            height: auto;
        }
    }
`;

const ErrorPlaceholder = styled.div<{ width?: string }>`
    width: ${(props) => props.width || 'auto'};
    height: auto;
    background: ${colors.gray[1500]};
    border-radius: 8px;
    color: ${colors.gray[1800]};
    display: flex;
    align-items: center;
    justify-content: center;
    border: 2px dashed ${colors.gray[1400]};
    font-size: 14px;
    min-height: 100px;
`;

export const LoadedImage = ({
    src,
    alt,
    width,
    errorMessage = 'Image not found',
    showErrorDetails = true,
    onLoad,
    onError,
    ...props
}: LoadedImageProps) => {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(false);

    const handleImageLoad = () => {
        setLoading(false);
        onLoad?.();
    };

    const handleImageError = () => {
        setLoading(false);
        setError(true);
        onError?.();
    };

    return (
        <ImageContainer width={width}>
            {loading && <ImageSkeleton width={width} active />}
            {!error && (
                <StyledImage
                    src={src}
                    alt={alt}
                    width={width}
                    onLoad={handleImageLoad}
                    onError={handleImageError}
                    style={{ display: loading ? 'none' : 'block' }}
                    {...props}
                />
            )}
            {error && (
                <ErrorPlaceholder width={width}>
                    {errorMessage}
                    {showErrorDetails && `: ${alt}`}
                </ErrorPlaceholder>
            )}
        </ImageContainer>
    );
};
