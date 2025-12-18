import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import backgroundVideo from '@images/signup-animation.mp4';

export const VideoWrapper = styled.div`
    position: relative;
    width: 100%;
    height: 100vh;
    overflow: hidden;
    background-color: ${colors.gray[1600]};
`;

const BackgroundVideo = styled.video`
    position: absolute;
    top: 50%;
    left: 50%;
    width: 100vw;
    height: 100vh;
    transform: translate(-50%, -50%);
    z-index: 1;
    object-fit: contain;
`;

export const Content = styled.div`
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
    return (
        <VideoWrapper>
            <BackgroundVideo src={backgroundVideo} autoPlay muted loop playsInline preload="auto" />
            <Content>{children}</Content>
        </VideoWrapper>
    );
}
