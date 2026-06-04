import { Heading, Text } from '@components';
import { Spin } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
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

export const VideoSlide: React.FC<VideoSlideProps> = ({ videoSrc, isReady, onVideoLoad, width }) => {
    const { t } = useTranslation('onboarding');
    return (
        <>
            {isReady ? (
                <StyledVideo width={width} autoPlay loop muted playsInline>
                    <source src={videoSrc} type="video/mp4" />
                </StyledVideo>
            ) : (
                <LoadingContainer width={width}>{t('welcome.loadingVideo')}</LoadingContainer>
            )}
            {videoSrc && !isReady && (
                <HiddenPreloadVideo width={width} autoPlay loop muted playsInline onCanPlay={onVideoLoad}>
                    <source src={videoSrc} type="video/mp4" />
                </HiddenPreloadVideo>
            )}
        </>
    );
};
