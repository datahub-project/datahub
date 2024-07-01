import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { Text, textDefaults } from '../../../src/components';

import { VerticalFlexGrid } from '../../docLayoutComponents/DocsComponents';

// More on how to set up stories at: https://storybook.js.org/docs/writing-stories#default-export
const meta = {
	title: 'Typography/Text',
	component: Text,
	tags: ['autodocs'],
	// Display Properties 
	parameters: {
		layout: 'centered',
		docs: {
			subtitle: 'Used to render text and paragraphs within an interface.',
		},
	},
	// Component-level argTypes
	argTypes: {
		children: {
			description: 'The content to display within the heading.',
			table: {
				type: { summary: 'string' },
			},
		},
		type: {
			description: 'The type of text to display.',
			table: {
				defaultValue: { summary: textDefaults.type }
			},
		},
		size: {
			description: 'Override the size of the text.',
			table: {
				defaultValue: { summary: textDefaults.size }
			},
		},
		color: {
			description: 'Override the color of the text.',
			table: {
				defaultValue: { summary: textDefaults.color }
			},
		},
		weight: {
			description: 'Override the weight of the heading.',
			table: {
				defaultValue: { summary: textDefaults.weight }
			},
		},
	},
	// Define default args
	args: {
		children: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas aliquet nulla id felis vehicula, et posuere dui dapibus. Nullam rhoncus massa non tortor convallis, in blandit turpis rutrum. Morbi tempus velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel mollis eros.',
		type: textDefaults.type,
		size: textDefaults.size,
		color: textDefaults.color,
		weight: textDefaults.weight,
	},
} satisfies Meta<typeof Text>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
	render: function Render(args) {
		return (
			<Text {...args} />
		);
	},
};

export const Sizes: Story = {
	render: function Render(args) {
		return (
			<VerticalFlexGrid>
				<Text size="4xl">{args.children}</Text>
				<Text size="3xl">{args.children}</Text>
				<Text size="2xl">{args.children}</Text>
				<Text size="xl">{args.children}</Text>
				<Text size="lg">{args.children}</Text>
				<Text size="md">{args.children}</Text>
				<Text size="sm">{args.children}</Text>
				<Text size="xs">{args.children}</Text>
			</VerticalFlexGrid>
		);
	},
};

export const WithLink: Story = {
	render: function Render(_) {
		return (
			<Text>
				Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas aliquet nulla id felis vehicula, et posuere dui dapibus. <a href="#">Nullam rhoncus massa non tortor convallis</a>, in blandit turpis rutrum. Morbi tempus velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel mollis eros.
			</Text>
		);
	},
};