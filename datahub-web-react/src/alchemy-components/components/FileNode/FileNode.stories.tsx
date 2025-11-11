import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { FileNode } from './FileNode';

const meta = {
    title: 'Components / FileNode',
    component: FileNode,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'FileNode displays a file with its icon, name, and optional actions like closing or custom right content.',
        },
    },

    // Component-level argTypes
    argTypes: {
        fileName: {
            description: 'Name of the file to display.',
            control: {
                type: 'text',
            },
        },
        loading: {
            description: 'Whether the file is in a loading state.',
            control: {
                type: 'boolean',
            },
        },
        border: {
            description: 'Whether to show a border around the FileNode.',
            control: {
                type: 'boolean',
            },
        },
        extraRightContent: {
            description: 'Extra content to show on the right side of the FileNode.',
            control: {
                type: 'text',
            },
        },
        className: {
            description: 'Additional CSS class name for the FileNode.',
            control: {
                type: 'text',
            },
        },
        size: {
            description: 'Size of the FileNode text.',
            options: ['xs', 'sm', 'md', 'lg', 'xl'],
            control: {
                type: 'select',
            },
        },
        onClick: {
            description: 'Function to call when the FileNode is clicked.',
            action: 'clicked',
        },
        onClose: {
            description: 'Function to call when the close button is clicked.',
            action: 'closed',
        },
    },

    // Define defaults
    args: {
        fileName: 'example-file.pdf',
        loading: false,
        border: false,
        size: 'md',
    },
} satisfies Meta<typeof FileNode>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <FileNode {...props} />,
};

export const withFileName = () => (
    <GridList>
        <FileNode fileName="document.pdf" />
        <FileNode fileName="presentation.pptx" />
        <FileNode fileName="spreadsheet.xlsx" />
        <FileNode fileName="image.png" />
    </GridList>
);

export const loadingState = () => (
    <GridList>
        <FileNode fileName="document.pdf" loading={true} />
    </GridList>
);

export const withBorder = () => (
    <GridList>
        <FileNode fileName="document.pdf" border={true} />
        <FileNode fileName="presentation.pptx" border={true} />
    </GridList>
);

export const withCloseButton = () => (
    <GridList>
        <FileNode 
            fileName="document.pdf" 
            onClose={() => console.log('Document closed')} 
        />
        <FileNode 
            fileName="presentation.pptx" 
            onClose={() => console.log('Presentation closed')} 
        />
    </GridList>
);

export const withOnClick = () => (
    <GridList>
        <FileNode 
            fileName="document.pdf" 
            onClick={() => console.log('Document clicked')} 
        />
        <FileNode 
            fileName="presentation.pptx" 
            onClick={() => console.log('Presentation clicked')} 
        />
    </GridList>
);

export const withExtraRightContent = () => (
    <GridList>
        <FileNode 
            fileName="document.pdf" 
            extraRightContent={<span style={{ color: 'green' }}>✓</span>}
        />
        <FileNode 
            fileName="presentation.pptx" 
            extraRightContent={<span style={{ color: 'blue' }}>!</span>}
        />
    </GridList>
);

export const withSizeVariations = () => (
    <GridList isVertical>
        <GridList>
            <FileNode fileName="xs-file.pdf" size="xs" />
            <FileNode fileName="sm-file.pdf" size="sm" />
        </GridList>
        <GridList>
            <FileNode fileName="md-file.pdf" size="md" />
            <FileNode fileName="lg-file.pdf" size="lg" />
            <FileNode fileName="xl-file.pdf" size="xl" />
        </GridList>
    </GridList>
);
