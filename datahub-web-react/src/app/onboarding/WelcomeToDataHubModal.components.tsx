import { Heading, Text } from '@components';
import { Spin } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
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

const TOUR_MEDIA_WIDTH_PX = 620;
const TOUR_MEDIA_HEIGHT_PX = Math.ceil((TOUR_MEDIA_WIDTH_PX * 720) / 1208);
const TOUR_SLIDE_MIN_HEIGHT_PX = 470;
const TOUR_TITLE_LINE_PX = 32;
const TOUR_DESCRIPTION_LINE_PX = 28;
// One slide, tall enough for a 2-line title and a 3-line description above the
// 620px video, plus the slide's bottom margin. Still well under two slides,
// so the list cannot open at the stacked height.
const TOUR_LIST_MAX_HEIGHT_PX =
    Math.max(
        TOUR_SLIDE_MIN_HEIGHT_PX,
        TOUR_TITLE_LINE_PX * 2 + 4 + TOUR_DESCRIPTION_LINE_PX * 3 + 16 + TOUR_MEDIA_HEIGHT_PX,
    ) + 32;

/**
 * Cap the list at one wrapped slide so the modal doesn't open at the stacked height of every slide.
 */
export const TourSlides = styled.div`
    .slick-list {
        max-height: ${TOUR_LIST_MAX_HEIGHT_PX}px !important;
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

const MediaBox = styled.div<{ $width: string }>`
    position: relative;
    display: block;
    width: ${(props) => props.$width};
    height: calc(${(props) => props.$width} * 720 / 1208);
    aspect-ratio: 1208 / 720;
    margin: 0 auto;
`;

const PosterImage = styled.img`
    display: block;
    width: 100%;
    height: 100%;
    object-fit: contain;
`;

const StyledVideo = styled.video`
    display: block;
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    object-fit: contain;
`;

interface VideoSlideProps {
    videoSrc?: string;
    posterSrc: string;
    width: string;
    /** The visible slide plays. A prefetched slide only downloads. */
    active?: boolean;
    onPlaying?: (event: React.SyntheticEvent<HTMLVideoElement>) => void;
    onTimeUpdate?: (event: React.SyntheticEvent<HTMLVideoElement>) => void;
    onEnded?: (event: React.SyntheticEvent<HTMLVideoElement>) => void;
    onError?: (event: React.SyntheticEvent<HTMLVideoElement>) => void;
}

// Paint the WebP poster as an <img> first so first-visit LCP can be that image.
// Mount the <video> only after the poster has painted and an MP4 URL is ready.
// src on the video element restarts resource selection when the MP4 URL arrives.
// A <source> child inserted after the first pass does not, so autoplay never starts.
// The video sits on top of the img, so it also gets the poster and stays transparent
// until a frame has decoded. Hiding the still at mount leaves an empty box.
export const VideoSlide: React.FC<VideoSlideProps> = ({
    videoSrc,
    posterSrc,
    width,
    active = true,
    onPlaying,
    onTimeUpdate,
    onEnded,
    onError,
}) => {
    const [posterReady, setPosterReady] = useState(false);
    const [frameReady, setFrameReady] = useState(false);
    const videoRef = useRef<HTMLVideoElement>(null);
    const showVideo = Boolean(posterReady && videoSrc);

    useEffect(() => {
        setFrameReady(false);
    }, [videoSrc]);

    // autoPlay only applies on mount. A slide that already has its MP4 has to be
    // started when it becomes the visible one, and paused when it is not, or a
    // hidden clip keeps playing.
    useEffect(() => {
        const video = videoRef.current;
        if (!video) return undefined;
        try {
            if (!active) {
                if (!video.paused) video.pause();
                return undefined;
            }
            video.play()?.catch(() => {});
        } catch {
            // jsdom does not implement media playback.
        }
        return undefined;
    }, [active, videoSrc]);

    return (
        <MediaBox $width={width}>
            <PosterImage src={posterSrc} alt="" width={1208} height={720} onLoad={() => setPosterReady(true)} />
            {showVideo ? (
                <StyledVideo
                    ref={videoRef}
                    src={videoSrc}
                    poster={posterSrc}
                    autoPlay={active}
                    muted
                    playsInline
                    preload="auto"
                    style={{ opacity: frameReady ? 1 : 0 }}
                    onLoadedData={() => setFrameReady(true)}
                    onPlaying={onPlaying}
                    onTimeUpdate={onTimeUpdate}
                    onEnded={onEnded}
                    onError={onError}
                />
            ) : null}
        </MediaBox>
    );
};
