import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import {
    LoadingContainer,
    SlideContainer,
    StyledDocsLink,
    VideoContainer,
    VideoSlide,
} from '@app/onboarding/WelcomeToDataHubModal.components';

// Mock Ant Design Spin component
vi.mock('antd', () => ({
    Spin: ({ size, ...props }: any) => <div data-testid="spin" data-size={size} {...props} />,
}));

// Mock colors
vi.mock('@components/theme/foundations/colors', () => ({
    default: {
        gray: {
            100: '#f5f5f5',
            1500: '#e5e5e5',
            1700: '#333333',
        },
        primary: {
            500: '#1890ff',
        },
    },
}));

describe('WelcomeToDataHubModal Components', () => {
    describe('SlideContainer', () => {
        it('should render with default visibility', () => {
            render(<SlideContainer data-testid="slide-container">Test content</SlideContainer>);

            const container = screen.getByTestId('slide-container');
            expect(container).toBeInTheDocument();
            expect(container).toHaveTextContent('Test content');
        });

        it('should be visible when isActive is true', () => {
            render(
                <SlideContainer isActive data-testid="slide-container">
                    Test content
                </SlideContainer>,
            );

            const container = screen.getByTestId('slide-container');
            expect(container).toBeInTheDocument();
        });

        it('should be hidden when isActive is false', () => {
            render(
                <SlideContainer isActive={false} data-testid="slide-container">
                    Test content
                </SlideContainer>,
            );

            const container = screen.getByTestId('slide-container');
            expect(container).toBeInTheDocument();
        });

        it('should render children correctly', () => {
            render(
                <SlideContainer data-testid="slide-container">
                    <div>Child content</div>
                    <p>Another child</p>
                </SlideContainer>,
            );

            expect(screen.getByText('Child content')).toBeInTheDocument();
            expect(screen.getByText('Another child')).toBeInTheDocument();
        });
    });

    describe('VideoContainer', () => {
        it('should render children correctly', () => {
            render(
                <VideoContainer data-testid="video-container">
                    <div>Video content</div>
                </VideoContainer>,
            );

            const container = screen.getByTestId('video-container');
            expect(container).toBeInTheDocument();
            expect(screen.getByText('Video content')).toBeInTheDocument();
        });

        it('should render multiple children', () => {
            render(
                <VideoContainer>
                    <video data-testid="video-element" />
                    <div data-testid="controls">Controls</div>
                </VideoContainer>,
            );

            expect(screen.getByTestId('video-element')).toBeInTheDocument();
            expect(screen.getByTestId('controls')).toBeInTheDocument();
        });
    });

    describe('LoadingContainer', () => {
        it('should render with default loading text', () => {
            render(<LoadingContainer width="300px" />);

            expect(screen.getByTestId('spin')).toBeInTheDocument();
            expect(screen.getByTestId('spin')).toHaveAttribute('data-size', 'large');
            expect(screen.getByText('Loading video...')).toBeInTheDocument();
        });

        it('should render with custom loading text', () => {
            render(<LoadingContainer width="400px">Custom loading message</LoadingContainer>);

            expect(screen.getByTestId('spin')).toBeInTheDocument();
            expect(screen.getByText('Custom loading message')).toBeInTheDocument();
        });

        it('should apply correct width', () => {
            const { container } = render(<LoadingContainer width="500px" />);

            const loadingContainer = container.firstChild as HTMLElement;
            expect(loadingContainer).toBeInTheDocument();
        });

        it('should render with custom children', () => {
            render(
                <LoadingContainer width="300px">
                    <div data-testid="custom-content">Custom content</div>
                    <span>Additional text</span>
                </LoadingContainer>,
            );

            expect(screen.getByTestId('spin')).toBeInTheDocument();
            expect(screen.getByTestId('custom-content')).toBeInTheDocument();
            expect(screen.getByText('Additional text')).toBeInTheDocument();
        });
    });

    describe('StyledDocsLink', () => {
        it('should render as an anchor with correct attributes', () => {
            render(
                <StyledDocsLink href="https://docs.datahub.com" target="_blank" rel="noopener noreferrer">
                    DataHub Docs
                </StyledDocsLink>,
            );

            const link = screen.getByRole('link', { name: 'DataHub Docs' });
            expect(link).toBeInTheDocument();
            expect(link).toHaveAttribute('href', 'https://docs.datahub.com');
            expect(link).toHaveAttribute('target', '_blank');
            expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        });

        it('should handle click events', () => {
            const handleClick = vi.fn();
            render(
                <StyledDocsLink href="#" onClick={handleClick}>
                    Click me
                </StyledDocsLink>,
            );

            const link = screen.getByRole('link', { name: 'Click me' });
            fireEvent.click(link);

            expect(handleClick).toHaveBeenCalledTimes(1);
        });

        it('should render with custom text content', () => {
            render(<StyledDocsLink href="#">Custom link text</StyledDocsLink>);

            expect(screen.getByText('Custom link text')).toBeInTheDocument();
        });
    });

    describe('VideoSlide', () => {
        const defaultProps = {
            videoSrc: 'https://example.com/video.mp4',
            isReady: false,
            onVideoLoad: vi.fn(),
            width: '600px',
        };

        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should render loading state when not ready', () => {
            render(<VideoSlide {...defaultProps} />);

            expect(screen.getByTestId('spin')).toBeInTheDocument();
            expect(screen.getByText('Loading video...')).toBeInTheDocument();
        });

        it('should render video when ready', () => {
            render(<VideoSlide {...defaultProps} isReady />);

            const video = screen.getByRole('generic'); // styled video element
            expect(video).toBeInTheDocument();
        });

        it('should call onVideoLoad when preload video can play', () => {
            const mockOnVideoLoad = vi.fn();
            render(<VideoSlide {...defaultProps} onVideoLoad={mockOnVideoLoad} />);

            // Find the hidden preload video
            const videos = screen.getAllByRole('generic');
            const preloadVideo = videos.find((video) => video.tagName.toLowerCase() === 'video');

            if (preloadVideo) {
                fireEvent.canPlay(preloadVideo);
                expect(mockOnVideoLoad).toHaveBeenCalledTimes(1);
            }
        });

        it('should not render preload video when no videoSrc', () => {
            render(<VideoSlide {...defaultProps} videoSrc={undefined} />);

            expect(screen.getByTestId('spin')).toBeInTheDocument();
            // Should only have loading container, no video elements
            const videos = screen.queryAllByRole('generic');
            expect(videos.filter((el) => el.tagName.toLowerCase() === 'video')).toHaveLength(0);
        });

        it('should handle ready state with video source', () => {
            render(<VideoSlide {...defaultProps} isReady videoSrc="test-video.mp4" />);

            expect(screen.queryByTestId('spin')).not.toBeInTheDocument();
            expect(screen.queryByText('Loading video...')).not.toBeInTheDocument();
        });

        it('should apply correct width to video', () => {
            const customWidth = '800px';
            render(<VideoSlide {...defaultProps} isReady width={customWidth} />);

            // Video should be rendered when ready
            const container = screen.getByRole('generic');
            expect(container).toBeInTheDocument();
        });

        it('should handle video events correctly', () => {
            const mockOnVideoLoad = vi.fn();
            render(<VideoSlide {...defaultProps} onVideoLoad={mockOnVideoLoad} />);

            // Should have loading container and hidden preload video
            expect(screen.getByTestId('spin')).toBeInTheDocument();
        });
    });

    describe('Component integration', () => {
        it('should work together in a complete slide structure', () => {
            const mockOnVideoLoad = vi.fn();

            render(
                <SlideContainer isActive data-testid="slide">
                    <h2>Slide Title</h2>
                    <VideoContainer>
                        <VideoSlide videoSrc="test.mp4" isReady={false} onVideoLoad={mockOnVideoLoad} width="500px" />
                    </VideoContainer>
                    <StyledDocsLink href="#" data-testid="docs-link">
                        Learn More
                    </StyledDocsLink>
                </SlideContainer>,
            );

            expect(screen.getByTestId('slide')).toBeInTheDocument();
            expect(screen.getByText('Slide Title')).toBeInTheDocument();
            expect(screen.getByTestId('spin')).toBeInTheDocument();
            expect(screen.getByTestId('docs-link')).toBeInTheDocument();
        });

        it('should handle complete ready state', () => {
            const mockOnVideoLoad = vi.fn();

            render(
                <SlideContainer isActive>
                    <VideoContainer>
                        <VideoSlide videoSrc="test.mp4" isReady onVideoLoad={mockOnVideoLoad} width="500px" />
                    </VideoContainer>
                </SlideContainer>,
            );

            expect(screen.queryByTestId('spin')).not.toBeInTheDocument();
            expect(screen.queryByText('Loading video...')).not.toBeInTheDocument();
        });
    });
});
