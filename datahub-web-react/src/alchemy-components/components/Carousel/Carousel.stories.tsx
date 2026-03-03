import { Carousel } from '@components';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

const meta = {
    title: 'Components / Carousel',
    component: Carousel,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A reusable carousel component with customizable styling and behavior.',
        },
    },
    tags: ['autodocs'],
    argTypes: {
        autoplay: {
            description: 'Whether the carousel should autoplay slides',
            control: 'boolean',
        },
        autoplaySpeed: {
            description: 'Speed of autoplay in milliseconds',
            control: 'number',
        },
        arrows: {
            description: 'Whether to show navigation arrows',
            control: 'boolean',
        },
        dots: {
            description: 'Whether to show dot indicators',
            control: 'boolean',
        },
        animateDot: {
            description: 'Whether to animate the active dot scaling from 0 to full size',
            control: 'boolean',
        },
        dotDuration: {
            description:
                'Duration in milliseconds for the dot scale animation. When autoplay is enabled, this automatically matches autoplaySpeed.',
            control: 'number',
        },
    },
} satisfies Meta<typeof Carousel>;

export default meta;
type Story = StoryObj<typeof meta>;

// Sample slide content
const SampleSlide = ({ title, color = '#f0f0f0' }: { title: string; color?: string }) => (
    <div
        style={{
            height: '200px',
            background: color,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            borderRadius: '8px',
            color: '#333',
            fontSize: '18px',
            fontWeight: 'bold',
        }}
    >
        {title}
    </div>
);

export const Basic: Story = {
    args: {
        autoplay: false,
        arrows: true,
        dots: true,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="Slide 1" color="#e6f3ff" />
                <SampleSlide title="Slide 2" color="#fff2e6" />
                <SampleSlide title="Slide 3" color="#f0ffe6" />
            </Carousel>
        </div>
    ),
};

export const Autoplay: Story = {
    args: {
        autoplay: true,
        autoplaySpeed: 2000,
        arrows: true,
        dots: true,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="Auto Slide 1" color="#ffe6e6" />
                <SampleSlide title="Auto Slide 2" color="#e6ffe6" />
                <SampleSlide title="Auto Slide 3" color="#e6e6ff" />
            </Carousel>
        </div>
    ),
};

export const WithoutArrows: Story = {
    args: {
        autoplay: false,
        arrows: false,
        dots: true,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="No Arrows 1" color="#ffebf0" />
                <SampleSlide title="No Arrows 2" color="#f0ffeb" />
                <SampleSlide title="No Arrows 3" color="#ebf0ff" />
            </Carousel>
        </div>
    ),
};

export const WithoutDots: Story = {
    args: {
        autoplay: false,
        arrows: true,
        dots: false,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="No Dots 1" color="#f5f0ff" />
                <SampleSlide title="No Dots 2" color="#fff0f5" />
                <SampleSlide title="No Dots 3" color="#f0fff5" />
            </Carousel>
        </div>
    ),
};

export const WithAnimatedDot: Story = {
    args: {
        autoplay: false,
        arrows: false,
        dots: true,
        animateDot: true,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="Manual Navigation" color="#e6f3ff" />
                <SampleSlide title="Click Dots" color="#ffe6f3" />
                <SampleSlide title="Watch Animation" color="#f3ffe6" />
            </Carousel>
            <p style={{ marginTop: '16px', textAlign: 'center', fontSize: '14px', color: '#666' }}>
                🖱️ Manual navigation: Click dots to see 3-second grow animation.
                <br />
                💡 For autoplay, use the &quot;AutoplayWithProgressDots&quot; story.
            </p>
        </div>
    ),
};

export const AutoplayWithProgressDots: Story = {
    args: {
        autoplay: true,
        autoplaySpeed: 3000,
        arrows: false,
        dots: true,
        infinite: false, // Stops on last slide instead of looping
        animateDot: true,
    },
    render: (args) => (
        <div style={{ width: '400px' }}>
            <Carousel {...args}>
                <SampleSlide title="Slide 1" color="#ffe6e6" />
                <SampleSlide title="Slide 2" color="#e6ffe6" />
                <SampleSlide title="Final Slide" color="#e6e6ff" />
            </Carousel>
            <p style={{ marginTop: '16px', textAlign: 'center', fontSize: '14px', color: '#666' }}>
                🎯 Autoplay: Dot animation duration automatically matches autoplay speed (3s).
                <br />
                ⏹️ Stops on last slide (no infinite loop).
            </p>
        </div>
    ),
};
