import React from 'react';
import styled, { css } from 'styled-components';

import { useIsDarkMode } from '@app/theme/useIsDarkMode';

import backgroundVideo from '@images/login-signup-animation.mp4';

const VideoWrapper = styled.div`
    position: relative;
    width: 100%;
    height: 100vh;
    overflow: hidden;
    background-color: ${(props) => props.theme.colors.bg};
`;

const BackgroundVideo = styled.video<{ $isDarkMode: boolean }>`
    position: absolute;
    top: 50%;
    left: 50%;
    width: 100vw;
    height: 100vh;
    transform: translate(-50%, -50%);
    z-index: 1;
    object-fit: cover;

    /* The animation has an off-white (not pure white) background baked into the video. In dark mode,
       invert it (hue-rotate keeps the line colors) and bump contrast so the inverted near-black
       background clips to pure black, then screen-blend so it falls through to the themed wrapper
       background while the lines stay visible. */
    ${(props) =>
        props.$isDarkMode &&
        css`
            filter: invert(1) hue-rotate(180deg) contrast(1.2);
            mix-blend-mode: screen;
        `}
`;

const Content = styled.div`
    position: relative;
    z-index: 2;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
`;

interface Props {
    children: React.ReactNode;
}

export default function AuthPageContainer({ children }: Props) {
    const [isDarkMode] = useIsDarkMode();

    return (
        <VideoWrapper>
            <BackgroundVideo
                src={backgroundVideo}
                $isDarkMode={isDarkMode}
                autoPlay
                muted
                loop
                playsInline
                preload="auto"
            />
            <Content>{children}</Content>
        </VideoWrapper>
    );
}
