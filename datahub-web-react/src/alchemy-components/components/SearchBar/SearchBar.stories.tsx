import React, { useState } from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { GridList } from '@components/.docs/mdx-components';
import { SearchBar, searchBarDefaults } from './SearchBar';
import { SearchBarProps } from './types';

const meta = {
    title: 'Components / Search Bar',
    component: SearchBar,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'A component that is used to get search bar',
        },
    },

    // Component-level argTypes
    argTypes: {
        placeholder: {
            description: 'Placeholder of search bar.',
            table: {
                defaultValue: { summary: searchBarDefaults.placeholder },
            },
            control: {
                type: 'text',
            },
        },
        value: {
            description: 'Value of the search bar.',
            table: {
                defaultValue: { summary: searchBarDefaults.value },
            },
            control: false,
        },
        width: {
            description: 'Width of the search bar.',
            table: {
                defaultValue: { summary: searchBarDefaults.width },
            },
            control: {
                type: 'text',
            },
        },
        allowClear: {
            description: 'Whether clear button should be present.',
            table: {
                defaultValue: { summary: searchBarDefaults.allowClear?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        onChange: {
            description: 'On change function for the search bar.',
        },
    },

    // Define defaults
    args: {
        placeholder: searchBarDefaults.placeholder,
        value: searchBarDefaults.value,
        allowClear: searchBarDefaults.allowClear,
        width: searchBarDefaults.width,
    },
} satisfies Meta<typeof SearchBar>;

export default meta;

type Story = StoryObj<typeof meta>;

const SandboxWrapper = (props: SearchBarProps) => {
    const [value, setValue] = useState('');

    const handleChange = (newValue: string) => {
        setValue(newValue);
    };

    return <SearchBar {...props} value={value} onChange={handleChange} />;
};

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => {
        return <SandboxWrapper {...props} />;
    },
};

export const customWidths = () => (
    <GridList isVertical>
        <SearchBar width="500px" />
        <SearchBar width="300px" />
        <SearchBar width="200px" />
        <SearchBar width="150px" />
    </GridList>
);
