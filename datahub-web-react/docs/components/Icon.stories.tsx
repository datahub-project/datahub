import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { Icon, AVAILABLE_ICONS } from '../../src/components';

import { GridList } from '../docLayoutComponents/GridList';

// More on how to set up stories at: https://storybook.js.org/docs/writing-stories#default-export
const meta = {
	title: 'Components/Icon',
	component: Icon,
	tags: ['autodocs'],
	// Display Properties 
	parameters: {
		layout: 'centered',
	},
	// Component-level argTypes
	argTypes: {
		icon: {
			description: `The name of the icon to display.`,
			type: 'string',
			options: AVAILABLE_ICONS,
			control: {
				type: 'select',
			}
		},
		variant: {
			description: 'The variant of the icon to display.',
			defaultValue: 'outline',
			options: ['outline', 'filled'],
		},
		size: {
			description: 'The size of the icon to display.',
			defaultValue: 'lg',
		},
		color: {
			description: 'The color of the icon to display.',
			options: ['inherit', 'white', 'black', 'violet', 'green', 'red', 'blue', 'gray'],
			type: 'string',
			control: {
				type: 'select',
			}
		},
	},
	// Define default args
	args: {
		icon: 'AccountCircle',
		variant: 'outline',
		size: '4xl',
		color: 'black',
	},
} satisfies Meta<typeof Icon>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
	render: function Render(args) {
		return (
			<Icon {...args} />
		);
	},
};

export const Filled: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Icon {...args} icon="AccountCircle" variant="filled" size="4xl" />
				<Icon {...args} icon="AddHome" variant="filled" size="4xl" />
				<Icon {...args} icon="AdminPanelSettings" variant="filled" size="4xl" />
			</GridList>
		);
	},
};

export const Sizes: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Icon {...args} size="xs" />
				<Icon {...args} size="sm" />
				<Icon {...args} size="md" />
				<Icon {...args} size="lg" />
				<Icon {...args} size="xl" />
				<Icon {...args} size="2xl" />
				<Icon {...args} size="3xl" />
				<Icon {...args} size="4xl" />
			</GridList>
		);
	},
};

export const Colors: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Icon {...args} color="white" />
				<Icon {...args} color="black" />
				<Icon {...args} color="violet" />
				<Icon {...args} color="green" />
				<Icon {...args} color="red" />
				<Icon {...args} color="blue" />
				<Icon {...args} color="gray" />
			</GridList>
		);
	},
};

export const Rotation: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Icon {...args} icon="ChevronLeft" />
				<Icon {...args} icon="ChevronLeft" rotate="90" />
				<Icon {...args} icon="ChevronLeft" rotate="180" />
				<Icon {...args} icon="ChevronLeft" rotate="270" />
			</GridList>
		);
	},
};