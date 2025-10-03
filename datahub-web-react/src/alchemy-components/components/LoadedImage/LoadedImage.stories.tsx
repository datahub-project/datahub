import { LoadedImage } from '@components';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

const meta = {
    title: 'Media / LoadedImage',
    component: LoadedImage,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A reusable image component with loading states, error handling, and skeleton placeholder.',
        },
    },
    tags: ['autodocs'],
    argTypes: {
        src: {
            description: 'Image source URL',
            control: 'text',
        },
        alt: {
            description: 'Alternative text for the image',
            control: 'text',
        },
        width: {
            description: 'Width of the image container',
            control: 'text',
        },
        errorMessage: {
            description: 'Custom error message when image fails to load',
            control: 'text',
        },
        showErrorDetails: {
            description: 'Whether to show the alt text in error message',
            control: 'boolean',
        },
    },
} satisfies Meta<typeof LoadedImage>;

export default meta;
type Story = StoryObj<typeof meta>;

// Valid image URL for testing
const SAMPLE_IMAGE_URL = 'https://picsum.photos/400/300';

export const Basic: Story = {
    args: {
        src: SAMPLE_IMAGE_URL,
        alt: 'Sample image',
        width: '400px',
    },
};

export const WithCustomWidth: Story = {
    args: {
        src: SAMPLE_IMAGE_URL,
        alt: 'Sample image with custom width',
        width: '600px',
    },
};

export const ErrorState: Story = {
    args: {
        src: 'https://invalid-url-that-will-fail.jpg',
        alt: 'This image will fail to load',
        width: '400px',
    },
};

export const ErrorStateWithCustomMessage: Story = {
    args: {
        src: 'https://invalid-url-that-will-fail.jpg',
        alt: 'This image will fail to load',
        width: '400px',
        errorMessage: 'Custom error: Failed to load image',
        showErrorDetails: false,
    },
};

export const LoadingState: Story = {
    args: {
        src: 'https://picsum.photos/800/600', // Larger image to show loading
        alt: 'Large image that will show loading state',
        width: '400px',
    },
    render: (args) => (
        <div>
            <p>This image should show a loading skeleton briefly while the image loads.</p>
            <LoadedImage {...args} />
        </div>
    ),
};
