import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';

import { VideoSlide } from '@app/onboarding/WelcomeToDataHubModal.components';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('VideoSlide', () => {
    it('paints the poster image first and mounts the video after it loads', () => {
        const { container, rerender } = render(
            <MockedProvider>
                <TestPageContainer>
                    <VideoSlide posterSrc="/poster.webp" width="620px" />
                </TestPageContainer>
            </MockedProvider>,
        );

        const poster = container.querySelector('img');
        expect(poster).toHaveAttribute('src', '/poster.webp');
        expect(container.querySelector('video')).toBeNull();

        rerender(
            <MockedProvider>
                <TestPageContainer>
                    <VideoSlide posterSrc="/poster.webp" videoSrc="/video.mp4" width="620px" />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Video stays unmounted until the poster has painted.
        expect(container.querySelector('video')).toBeNull();

        fireEvent.load(container.querySelector('img') as HTMLImageElement);

        const video = container.querySelector('video');
        expect(video).not.toBeNull();
        expect(video).toHaveAttribute('src', '/video.mp4');
        expect(video).not.toHaveAttribute('width');
    });
});
