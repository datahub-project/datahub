import type { Meta, StoryObj } from '@storybook/react';
import React, { useEffect, useState } from 'react';

import { LoadedVideo } from '@src/alchemy-components/components/LoadedVideo/LoadedVideo';
import {
    CarouselContainer,
    IndicatorContainer,
    IndicatorText,
    SlideContainer,
    SlideTitle,
    StoryContainer,
    StoryDescription,
    StoryTitle,
    VideoContainer,
    VideoPlayerContainer,
} from '@src/alchemy-components/components/LoadedVideo/LoadedVideo.components';

import searchVideo from '@images/FTE-search.mp4';

const SAMPLE_VIDEO_URL = searchVideo;
const MISSING_VIDEO_URL = '/assets/does-not-exist.mp4';

// A MediaSource that never receives data keeps the video in its loading state indefinitely.
function NeverLoadingVideo(props: React.ComponentProps<typeof LoadedVideo>) {
    const [src, setSrc] = useState<string>();
    useEffect(() => {
        const url = URL.createObjectURL(new MediaSource());
        setSrc(url);
        return () => URL.revokeObjectURL(url);
    }, []);
    if (!src) return null;
    return <LoadedVideo {...props} mp4Src={src} />;
}

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
    // NeverLoadingVideo supplies its own source, so the mp4Src control has no effect here.
    argTypes: { mp4Src: { control: false } },
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
        <StoryContainer>
            <StoryTitle>Loading State Demo</StoryTitle>
            <StoryDescription>The source never delivers data, so the loading spinner stays visible.</StoryDescription>
            <NeverLoadingVideo {...args} height="225px" />
        </StoryContainer>
    ),
};

export const ErrorState: Story = {
    args: {
        mp4Src: MISSING_VIDEO_URL,
        width: '400px',
        autoplay: true,
        loop: true,
        muted: true,
        controls: false,
        playsInline: true,
    },
    render: (args) => (
        <StoryContainer>
            <StoryTitle>Error State Demo</StoryTitle>
            <StoryDescription>
                This demonstrates the error state when a video fails to load.
                <br />
                Using a missing video URL to trigger the error state.
            </StoryDescription>
            <LoadedVideo {...args} height="225px" />
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
                    Demonstrating the LoadedVideo component with a sample video.
                    <br />
                    This video will automatically loop and play.
                </StoryDescription>
                <CarouselContainer>
                    <SlideContainer>
                        <SlideTitle>Sample Video</SlideTitle>
                        <VideoContainer>
                            <VideoPlayerContainer>
                                <LoadedVideo
                                    mp4Src={args.mp4Src}
                                    width={args.width}
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
                    <IndicatorText>Sample video bundled with DataHub (src/images)</IndicatorText>
                </IndicatorContainer>
            </StoryContainer>
        );
    },
};
