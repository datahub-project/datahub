import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { Icon } from '@components/components/Icon';

import { GridList } from '@src/alchemy-components/.docs/mdx-components';
import { colors } from '@src/alchemy-components/theme';

import { Card, cardDefaults } from '.';

// Auto Docs
const meta = {
    title: 'Components / Card',
    component: Card,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render a card.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'The title of the card',
            table: {
                defaultValue: { summary: `${cardDefaults.title}` },
            },
            control: {
                type: 'text',
            },
        },
        subTitle: {
            description: 'The subtitle of the card',
            control: {
                type: 'text',
            },
        },
        icon: {
            description: 'The icon on the card',
            control: {
                type: 'text',
            },
        },
        iconAlignment: {
            description: 'Whether the alignment of icon is horizontal or vertical',
            table: {
                defaultValue: { summary: `${cardDefaults.iconAlignment}` },
            },
            control: {
                type: 'select',
            },
        },
        percent: {
            description: 'The percent value on the pill of the card',
            control: {
                type: 'number',
            },
        },
        button: {
            description: 'The button on the card',
            control: {
                type: 'text',
            },
        },
        width: {
            description: 'The width of the card',
            control: {
                type: 'text',
            },
        },
        maxWidth: {
            description: 'The maximum width of the card',
            control: {
                type: 'text',
            },
        },
        height: {
            description: 'The height of the card',
            control: {
                type: 'text',
            },
        },
        onClick: {
            description: 'The on click function for the card',
        },
        isEmpty: {
            description: 'Whether the card is in empty state with No data',
            table: {
                defaultValue: { summary: `${cardDefaults.isEmpty}` },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define default args
    args: {
        title: 'Title',
        subTitle: 'Subtitle',
        iconAlignment: 'horizontal',
        isEmpty: false,
    },
} satisfies Meta<typeof Card>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Card {...props} />,
};

export const withChildren = () => (
    <Card title="Title" subTitle="Subtitle">
        <div style={{ backgroundColor: colors.gray[1000], padding: '8px 32px' }}>Children of the card (Swap me)</div>
    </Card>
);

export const withoutSubtitle = () => (
    <Card title="Title">
        <div style={{ backgroundColor: colors.gray[1000], padding: '8px 32px' }}>Children of the card (Swap me)</div>
    </Card>
);

export const withIcon = () => (
    <GridList>
        <Card title="Title" subTitle="Subtitle" icon={<Icon icon="Cloud" color="gray" />} />
        <Card title="Title" subTitle="Subtitle" icon={<Icon icon="Cloud" color="gray" />} iconAlignment="vertical" />
    </GridList>
);

export const withButton = () => (
    <Card
        title="Title"
        subTitle="Subtitle"
        button={<Icon icon="Download" color="gray" size="2xl" />}
        onClick={() => window.alert('Card clicked')}
    />
);

export const withPercentPill = () => <Card title="Title" subTitle="Subtitle" percent={2} />;

export const withAllTheElements = () => (
    <Card
        title="Title"
        subTitle="Subtitle"
        percent={2}
        icon={<Icon icon="Cloud" color="gray" />}
        button={<Icon icon="Download" color="gray" size="2xl" />}
        onClick={() => window.alert('Card clicked')}
    >
        <div style={{ backgroundColor: colors.gray[1000], padding: '8px 32px' }}>Children of the card (Swap me)</div>
    </Card>
);
