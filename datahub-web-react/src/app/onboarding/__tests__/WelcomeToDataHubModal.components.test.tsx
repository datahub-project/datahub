import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { VideoSlide } from '@app/onboarding/WelcomeToDataHubModal.components';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('VideoSlide', () => {
    it('keeps the poster video mounted when its source becomes available', () => {
        const { container, rerender } = render(
            <MockedProvider>
                <TestPageContainer>
                    <VideoSlide posterSrc="/poster.webp" width="620px" />
                </TestPageContainer>
            </MockedProvider>,
        );
        const video = container.querySelector('video');

        expect(video).toHaveAttribute('poster', '/poster.webp');
        expect(video?.querySelector('source')).not.toBeInTheDocument();

        rerender(
            <MockedProvider>
                <TestPageContainer>
                    <VideoSlide posterSrc="/poster.webp" videoSrc="/video.mp4" width="620px" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container.querySelector('video')).toBe(video);
        expect(video?.querySelector('source')).toHaveAttribute('src', '/video.mp4');
    });
});
