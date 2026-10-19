import { Heading, Text } from '@components';
import { Spin } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { typography } from '@components/theme';

/**
 * Container for individual carousel slides with centered content
 */
export const SlideContainer = styled.div`
    position: relative; /* Provide positioning context for absolutely positioned children */
    text-align: left;
    margin-bottom: 32px;
    min-height: 470px;
`;

const SlideTitleWrapper = styled.div`
    margin: 0 0 4px;
`;

export const SlideTitle: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <SlideTitleWrapper>
        <Heading type="h2" size="2xl" weight="bold">
            {children}
        </Heading>
    </SlideTitleWrapper>
);

export const SlideDescription: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <Text type="p" size="lg" color="textSecondary">
        {children}
    </Text>
);

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
 * Holds the tour carousel at the height slick settles on.
 *
 * Before slick finishes measuring, every slide is in normal flow, including the
 * 3376×2000 closing screenshot. The centered modal is then ~3200px tall, the
 * visible video sits below the viewport, and it jumps in when the track
 * collapses to one row. That jump is the first-visit CLS (the video enters the
 * viewport mid zoom). Cap the list at the settled height — the closing slide,
 * 539px at a 620px image — so the modal opens already centered.
 */
export const TourSlides = styled.div`
    .slick-list {
        max-height: 539px !important;
        overflow: hidden !important;
    }
`;

const LoadingContainerBase = styled.div<{ width: string }>`
    width: ${(props) => props.width};
    height: 350px; /* Match video aspect ratio for 620px width */
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 2px dashed ${(props) => props.theme.colors.border};
    border-radius: 8px;
    margin: 0 auto;
    gap: 16px;
`;

export const LoadingContainer: React.FC<{ width: string; children?: React.ReactNode }> = ({ width, children }) => (
    <LoadingContainerBase width={width}>
        <Spin size="large" />
        <Text size="lg" weight="medium" color="textSecondary">
            {children}
        </Text>
    </LoadingContainerBase>
);

export const StyledDocsLink = styled.a`
    color: ${(props) => props.theme.colors.textBrand};
    text-align: center;
    font-size: ${typography.fontSizes.md};
    font-weight: ${typography.fontWeights.semiBold};
    line-height: normal;
    letter-spacing: -0.07px;
    text-decoration: none;
    cursor: pointer;
    border-radius: 4px;
    padding: 10px 12px;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const StyledVideo = styled.video<{ $width: string }>`
    display: block;
    width: ${(props) => props.$width};
    /* Lock the box before metadata arrives. A bare <video> is 300×150 until the
       MP4 loads, and that jump is what pushed first-visit CLS over 0.1. */
    height: calc(${(props) => props.$width} * 720 / 1208);
    aspect-ratio: 1208 / 720;
    object-fit: contain;
`;

interface VideoSlideProps {
    videoSrc?: string;
    posterSrc: string;
    width: string;
}

// src on the video element restarts resource selection when the MP4 URL arrives.
// A <source> child inserted after the first pass does not, so autoplay never starts.
// $width stays off the DOM: the width attribute would otherwise be the string "620px".
export const VideoSlide: React.FC<VideoSlideProps> = ({ videoSrc, posterSrc, width }) => (
    <StyledVideo $width={width} poster={posterSrc} src={videoSrc || undefined} autoPlay loop muted playsInline />
);
