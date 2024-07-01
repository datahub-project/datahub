import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { Button, buttonDefaults, AVAILABLE_ICONS } from '../../src/components';

import { GridList } from '../docLayoutComponents/GridList';

// More on how to set up stories at: https://storybook.js.org/docs/writing-stories#default-export
const meta = {
	title: 'Components/Button',
	component: Button,
	subcomponents: undefined,
	tags: ['autodocs'],
	// Display Properties 
	parameters: {
		layout: 'centered',
	},
	// Component-level argTypes
	argTypes: {
		children: {
			description: 'The content of the Button.',
		},
		variant: {
			description: 'The variant of the Button.',
			options: ['filled', 'outline', 'text'],
			table: {
				defaultValue: { summary: buttonDefaults.variant }
			}
		},
		color: {
			description: 'The color of the Button.',
			table: {
				defaultValue: { summary: buttonDefaults.color }
			},
			control: {
				type: 'select',
			},
		},
		size: {
			description: 'The size of the Button.',
			table: {
				defaultValue: { summary: buttonDefaults.size }
			},
		},
		icon: {
			description: 'The icon to display in the Button.',
			options: AVAILABLE_ICONS,
			table: {
				defaultValue: { summary: 'undefined' }
			},
			control: {
				type: 'select',
			},
		},
		iconPosition: {
			description: 'The position of the icon in the Button.',
			table: {
				defaultValue: { summary: buttonDefaults.iconPosition }
			},
		},
		isCircle: {
			description: 'Whether the Button should be a circle. If this is selected, the Button will ignore children content, so add an Icon to the Button.',
			table: {
				defaultValue: { summary: buttonDefaults?.isCircle?.toString() }
			},
		},
		isLoading: {
			description: 'Whether the Button is in a loading state.',
			table: {
				defaultValue: { summary: buttonDefaults?.isLoading?.toString() }
			},
		},
		isDisabled: {
			description: 'Whether the Button is disabled.',
			table: {
				defaultValue: { summary: buttonDefaults?.isDisabled?.toString() }
			},
		},
		isActive: {
			description: 'Whether the Button is active.',
			table: {
				defaultValue: { summary: buttonDefaults?.isActive?.toString() }
			},
		},
		onClick: {
			description: 'Function to call when the button is clicked',
			table: {
				defaultValue: { summary: 'undefined' }
			},
			action: 'clicked'
		},
	},
	// Define default args
	args: {
		children: 'Button Content',
		variant: buttonDefaults.variant,
		color: buttonDefaults.color,
		size: buttonDefaults.size,
		icon: undefined,
		iconPosition: buttonDefaults.iconPosition,
		isCircle: buttonDefaults.isCircle,
		isLoading: buttonDefaults.isLoading,
		isDisabled: buttonDefaults.isDisabled,
		isActive: buttonDefaults.isActive,
		onClick: () => console.log('Button clicked'),
	},
} satisfies Meta<typeof Button>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
	render: function Render(args) {
		return (
			<Button {...args}>Default Button</Button>
		);
	},
};

export const States: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args}>Default</Button>
				<Button {...args} isLoading>Loading State</Button>
				<Button {...args} isActive>Active/Focus State</Button>
				<Button {...args} isDisabled>Disabled State</Button>
			</GridList>
		);
	},
};

export const Colors: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args}>Violet Button</Button>
				<Button {...args} color="green">Green Button</Button>
				<Button {...args} color="red">Red Button</Button>
				<Button {...args} color="blue">Blue Button</Button>
				<Button {...args} color="gray">Gray Button</Button>
			</GridList>
		);
	},
};

export const Sizes: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args} size="sm">Small Button</Button>
				<Button {...args} size="md">Regular Button</Button>
				<Button {...args} size="lg">Large Button</Button>
				<Button {...args} size="xl">XLarge Button</Button>
			</GridList>
		);
	},
};

export const WithIcon: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args} icon="Add">Icon Left</Button>
				<Button {...args} icon="Add" iconPosition="right">Icon Right</Button>
			</GridList>
		);
	},
};

export const CircleShape: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args} icon="Add" isCircle={true} size="sm" />
				<Button {...args} icon="Add" isCircle={true} />
				<Button {...args} icon="Add" isCircle={true} size="lg" />
			</GridList>
		);
	},
};