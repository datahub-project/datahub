import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { LoadedVideo } from '@src/alchemy-components/components/LoadedVideo/LoadedVideo';
import {
    CarouselContainer,
    ErrorContainer,
    IndicatorContainer,
    IndicatorText,
    LoadingContainer,
    SlideContainer,
    SlideTitle,
    StoryContainer,
    StoryDescription,
    StoryTitle,
    VideoContainer,
    VideoPlayerContainer,
} from '@src/alchemy-components/components/LoadedVideo/LoadedVideo.components';
import { VIDEO_WIDTH } from '@src/alchemy-components/components/LoadedVideo/constants';

const SAMPLE_VIDEO_URL = 'https://www.sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4';

const meta = {
    title: 'Example/LoadedVideo',
    component: LoadedVideo,
    parameters: {
        layout: 'centered',
    },
    tags: ['autodocs'],
    argTypes: {
        mp4Src: {
            control: 'text',
            description: 'URL to the MP4 video file',
        },
        webmSrc: {
            control: 'text',
            description: 'URL to the WebM video file (optional)',
        },
        posterSrc: {
            control: 'text',
            description: 'URL to the poster image displayed before video loads',
        },
        width: {
            control: 'text',
            description: 'Width of the video (CSS value)',
        },
        height: {
            control: 'text',
            description: 'Height of the video (CSS value)',
        },
        autoplay: {
            control: 'boolean',
            description: 'Whether the video should autoplay',
        },
        loop: {
            control: 'boolean',
            description: 'Whether the video should loop',
        },
        muted: {
            control: 'boolean',
            description: 'Whether the video should be muted',
        },
        controls: {
            control: 'boolean',
            description: 'Whether to show video controls',
        },
        playsInline: {
            control: 'boolean',
            description: 'Whether the video should play inline on mobile',
        },
    },
} satisfies Meta<typeof LoadedVideo>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        width: '400px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (args) => (
        <VideoPlayerContainer>
            <LoadedVideo {...args} />
        </VideoPlayerContainer>
    ),
};

export const WithControls: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        width: '400px',
        autoplay: false,
        loop: true,
        muted: false,
        controls: true,
        playsInline: true,
    },
    render: (args) => (
        <VideoPlayerContainer>
            <LoadedVideo {...args} />
        </VideoPlayerContainer>
    ),
};

export const CustomSize: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        width: '300px',
        height: '200px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (args) => (
        <VideoPlayerContainer>
            <LoadedVideo {...args} />
        </VideoPlayerContainer>
    ),
};

export const NoAutoplay: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        width: '400px',
        autoplay: false,
        loop: true,
        muted: true,
        controls: true,
        playsInline: true,
    },
    render: (args) => (
        <VideoPlayerContainer>
            <LoadedVideo {...args} />
        </VideoPlayerContainer>
    ),
};

export const WithPoster: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        posterSrc: 'https://via.placeholder.com/400x300/cccccc/666666?text=Video+Poster',
        width: '400px',
        autoplay: false,
        loop: true,
        muted: true,
        controls: true,
        playsInline: true,
    },
    render: (args) => (
        <VideoPlayerContainer>
            <LoadedVideo {...args} />
        </VideoPlayerContainer>
    ),
};

export const LoadingState: Story = {
    args: {
        mp4Src: 'https://httpstat.us/200?sleep=3000', // Simulates slow loading
        width: '400px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (_args) => (
        <StoryContainer>
            <StoryTitle>Loading State Demo</StoryTitle>
            <StoryDescription>
                This demonstrates the loading state while the video loads.
                <br />
                The video has a 3-second delay to simulate slow loading.
            </StoryDescription>
            <LoadingContainer>Loading video... Please wait</LoadingContainer>
            <IndicatorContainer>
                <IndicatorText>Simulated with 3-second delay via httpstat.us</IndicatorText>
            </IndicatorContainer>
        </StoryContainer>
    ),
};

export const ErrorState: Story = {
    args: {
        mp4Src: 'https://invalid-url.com/nonexistent-video.mp4',
        width: '400px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (_args) => (
        <StoryContainer>
            <StoryTitle>Error State Demo</StoryTitle>
            <StoryDescription>
                This demonstrates the error state when a video fails to load.
                <br />
                Using an invalid URL to trigger the error state.
            </StoryDescription>
            <ErrorContainer>⚠️ Failed to load video</ErrorContainer>
            <IndicatorContainer>
                <IndicatorText>Simulated with invalid video URL</IndicatorText>
            </IndicatorContainer>
        </StoryContainer>
    ),
};

export const SingleVideoDemo: Story = {
    args: {
        mp4Src: SAMPLE_VIDEO_URL,
        width: '520px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (args) => {
        return (
            <StoryContainer>
                <StoryTitle>Single Video Demo</StoryTitle>
                <StoryDescription>
                    Demonstrating the LoadedVideo component with a sample video from sample-videos.com.
                    <br />
                    This video will automatically loop and play.
                </StoryDescription>
                <CarouselContainer>
                    <SlideContainer>
                        <SlideTitle>Big Buck Bunny Sample Video</SlideTitle>
                        <VideoContainer>
                            <VideoPlayerContainer>
                                <LoadedVideo
                                    mp4Src={args.mp4Src}
                                    width={VIDEO_WIDTH}
                                    autoplay={args.autoplay}
                                    loop={args.loop}
                                    muted={args.muted}
                                    controls={args.controls}
                                    playsInline={args.playsInline}
                                />
                            </VideoPlayerContainer>
                        </VideoContainer>
                    </SlideContainer>
                </CarouselContainer>
                <IndicatorContainer>
                    <IndicatorText>
                        Sample video courtesy of sample-videos.com - Big Buck Bunny (1MB, 720p)
                    </IndicatorText>
                </IndicatorContainer>
            </StoryContainer>
        );
    },
};
