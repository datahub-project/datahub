import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { Entity, EntityType, Tag as TagType } from '@src/types.generated';
import { MockedProvider } from '@apollo/client/testing';
import Tag from '@src/app/sharedV2/tags/tag/Tag';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { Button } from 'antd';
import {
    defaultGlossaryTermEntities,
    defaultTagEntities,
    mockGlossaryTermEntityRegistry,
    mockGlossaryTermSelectedEntities,
    mockTagEntityRegistry,
    mockTagSelectedEntities,
    tagMocks,
} from './__mock.data';
import { SelectItemPopoverProps, SelectItemPopover } from './SelectItemPopover';

// Storybook meta configuration
const meta: Meta<SelectItemPopoverProps> = {
    title: 'Component / Select Items Popover',
    component: SelectItemPopover,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle:
                'A component that is used to select item on popover by just passing the Entity Type and default values',
        },
    },
    decorators: [
        (Story) => (
            <MockedProvider mocks={tagMocks} addTypename={false}>
                <EntityRegistryContext.Provider value={mockTagEntityRegistry}>
                    <Story />
                </EntityRegistryContext.Provider>
            </MockedProvider>
        ),
    ],
    // Component-level argTypes
    argTypes: {
        children: {
            description: 'The trigger element for the popover. This component will display the popover when clicked.',
            table: {
                defaultValue: { summary: '<Button>Click Me</Button>' },
            },
            control: { type: 'object' },
        },
        handleSelectionChange: {
            description: 'Callback function triggered when selected items are updated.',
            table: {
                type: { summary: '(params: { addedItems: Entity[], removedItems: Entity[] }) => void' },
            },
            control: false,
        },
        refetch: {
            description: 'Function to trigger a refetch of data when needed.',
            table: {
                type: { summary: '() => void' },
            },
            control: false,
        },
        onClose: {
            description: 'Callback function triggered when the popover is closed.',
            table: {
                type: { summary: '() => void' },
            },
            control: false,
        },
        entities: {
            description: 'List of available entity options to select from.',
            table: {
                type: { summary: 'Entity[]' },
            },
            control: { type: 'object' },
        },
        selectedItems: {
            description: 'List of entities that are currently selected.',
            table: {
                type: { summary: 'Entity[]' },
            },
            control: { type: 'object' },
        },
        entityType: {
            description: 'Type of entity being selected, e.g., Tag or GlossaryTerm.',
            table: {
                type: { summary: 'EntityType' },
                defaultValue: { summary: 'EntityType.Tag' },
            },
            control: {
                type: 'select',
                options: Object.values(EntityType),
            },
        },
        renderOption: {
            description: 'Custom renderer function for each option in the select list.',
            table: {
                type: { summary: '(option: { item: Entity }) => React.ReactNode' },
            },
            control: false,
        },
    },
    args: {
        children: <Button>Open Popover</Button>,
        handleSelectionChange: ({ selectedItems, removedItems }) => {
            console.log('Newly Selected Items:', selectedItems);
            console.log('Removed Items:', removedItems);
        },
        refetch: () => console.log('Refetch triggered'),
        onClose: () => console.log('Close triggered'),
    },
    tags: ['autodocs'],
};

export default meta;
type Story = StoryObj<SelectItemPopoverProps>;

// Story with Tag Selector
export const TagSelector: Story = {
    decorators: [
        (Story) => (
            <MockedProvider mocks={tagMocks} addTypename={false}>
                <EntityRegistryContext.Provider value={mockTagEntityRegistry}>
                    <Story />
                </EntityRegistryContext.Provider>
            </MockedProvider>
        ),
    ],
    args: {
        entities: defaultTagEntities as unknown as Entity[],
        selectedItems: mockTagSelectedEntities,
        entityType: EntityType.Tag,
        renderOption: (option) => {
            return (
                <Tag
                    tag={{ tag: option.item as TagType, associatedUrn: option.item?.urn }}
                    options={{ shouldNotOpenDrawerOnClick: true }}
                    maxWidth={120}
                />
            );
        },
    },
};

// Story with GlossaryTerm Selector
export const GlossaryTermSelectorWithoutCustomizeRenderingOptions: Story = {
    decorators: [
        (Story) => (
            <MockedProvider mocks={tagMocks} addTypename={false}>
                <EntityRegistryContext.Provider value={mockGlossaryTermEntityRegistry}>
                    <Story />
                </EntityRegistryContext.Provider>
            </MockedProvider>
        ),
    ],
    args: {
        entities: defaultGlossaryTermEntities as unknown as Entity[],
        selectedItems: mockGlossaryTermSelectedEntities,
        entityType: EntityType.GlossaryTerm,
    },
};
