import { Spin } from 'antd';
import React from 'react';
import styled from 'styled-components';

import colors from '@components/theme/foundations/colors';

/**
 * Container for individual carousel slides with centered content
 */
export const SlideContainer = styled.div<{ isActive?: boolean }>`
    position: relative; /* Provide positioning context for absolutely positioned children */
    text-align: left;
    margin-bottom: 32px;
    min-height: 470px;
    visibility: ${(props) => (props.isActive === false ? 'hidden' : 'visible')};
`;

/**
 * Container for video elements with centered alignment
 */
export const VideoContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding-top: 16px;
`;

/**
 * Styled loading container base
 */
const LoadingContainerBase = styled.div<{ width: string }>`
    width: ${(props) => props.width};
    height: 350px; /* Match video aspect ratio for 620px width */
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background-color: ${colors.gray[1500]};
    border: 2px dashed ${colors.gray[100]};
    border-radius: 8px;
    font-size: 16px;
    color: ${colors.gray[1700]};
    font-weight: 500;
    margin: 0 auto;
    gap: 16px;
`;

/**
 * Loading state container with Spin component
 * @param width - CSS width value for the container
 * @param children - Optional loading text
 */
export const LoadingContainer: React.FC<{ width: string; children?: React.ReactNode }> = ({
    width,
    children = 'Loading video...',
}) => (
    <LoadingContainerBase width={width}>
        <Spin size="large" />
        {children}
    </LoadingContainerBase>
);

/**
 * Styled anchor for DataHub Docs link
 */
export const StyledDocsLink = styled.a`
    color: ${colors.primary[500]};
    text-align: center;
    font-size: 14px;
    font-style: normal;
    font-weight: 650;
    line-height: normal;
    letter-spacing: -0.07px;
    text-decoration: none;
    cursor: pointer;
    border-radius: 4px;
    padding: 10px 12px;

    &:hover {
        background-color: ${colors.gray[1500]};
    }
`;

/**
 * Styled video element for visible videos
 */
const StyledVideo = styled.video<{ width: string }>`
    width: ${(props) => props.width};
`;

/**
 * Styled video element for hidden preload videos
 */
const HiddenPreloadVideo = styled.video<{ width: string }>`
    width: ${(props) => props.width};
    opacity: 0;
    position: absolute;
    pointer-events: none;
    top: 0;
    left: 0;
`;

/**
 * Reusable video slide component that handles loading states
 */
interface VideoSlideProps {
    videoSrc?: string;
    isReady: boolean;
    onVideoLoad: () => void;
    width: string;
}

export const VideoSlide: React.FC<VideoSlideProps> = ({ videoSrc, isReady, onVideoLoad, width }) => (
    <>
        {isReady ? (
            <StyledVideo width={width} autoPlay loop muted playsInline>
                <source src={videoSrc} type="video/mp4" />
            </StyledVideo>
        ) : (
            <LoadingContainer width={width}>Loading video...</LoadingContainer>
        )}
        {videoSrc && !isReady && (
            <HiddenPreloadVideo width={width} autoPlay loop muted playsInline onCanPlay={onVideoLoad}>
                <source src={videoSrc} type="video/mp4" />
            </HiddenPreloadVideo>
        )}
    </>
);
