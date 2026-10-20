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
        const still = container.querySelector('img');
        expect(video).not.toBeNull();
        expect(video).toHaveAttribute('src', '/video.mp4');
        expect(video).toHaveAttribute('poster', '/poster.webp');
        expect(video).not.toHaveAttribute('loop');
        expect(video).not.toHaveAttribute('width');
        // The still stays up until a frame has decoded. The video covers the img.
        expect(still).toBeVisible();
        expect(video).toHaveStyle({ opacity: '0' });

        fireEvent.loadedData(video as HTMLVideoElement);

        expect(still).toBeVisible();
        expect(video).toHaveStyle({ opacity: '1' });
    });

    it('reports when the clip ends', () => {
        const onEnded = vi.fn();
        const { container } = render(
            <MockedProvider>
                <TestPageContainer>
                    <VideoSlide posterSrc="/poster.webp" videoSrc="/video.mp4" width="620px" onEnded={onEnded} />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.load(container.querySelector('img') as HTMLImageElement);
        fireEvent.ended(container.querySelector('video') as HTMLVideoElement);

        expect(onEnded).toHaveBeenCalledTimes(1);
    });
});
