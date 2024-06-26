import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { PlusOutlined } from '@ant-design/icons';

import { Button } from '../../src/components';

import { GridList } from '../docLayoutComponents/GridList';

// More on how to set up stories at: https://storybook.js.org/docs/writing-stories#default-export
const meta = {
	title: 'Components/Button',
	component: Button,
	tags: ['autodocs'],
	// Display Properties 
	parameters: {
		layout: 'centered',
	},
	// Component-level argTypes
	argTypes: {
		children: {
			control: {
				type: 'text',
			},
		},
		variant: {
			description: 'The variant of the Button.',
			defaultValue: 'filled',
			control: {
				type: 'select',
			},
		},
		color: {
			description: 'The color of the Button.',
			defaultValue: 'violet',
			control: {
				type: 'select',
			},
		},
		size: {
			description: 'The size of the Button.',
			defaultValue: 'md',
			control: {
				type: 'select',
			},
		},
		isCircle: {
			description: 'Whether the Button should be a circle. If this is selected, the Button will ignore children content, so add an Icon to the Button.',
			defaultValue: false,
			control: {
				type: 'boolean',
			},
		},
		icon: {
			description: 'The icon to display in the Button.',
			control: {
				type: 'select',
			},
		},
		iconPosition: {
			description: 'The position of the icon in the Button.',
			defaultValue: 'left',
			control: {
				type: 'select',
			},
		},
		isLoading: {
			description: 'Whether the Button is in a loading state.',
			defaultValue: false,
			control: {
				type: 'boolean',
			},
		},
		disabled: {
			description: 'Whether the Button is disabled.',
			defaultValue: false,
			control: {
				type: 'boolean',
			},
		},
		onClick: { action: 'clicked' },
	},
	// Define default args
	args: {
		variant: 'filled',
		color: 'violet',
		size: 'md',
		isCircle: false,
		children: 'Button Content',
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

export const Colors: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args}>Violet Button</Button>
				<Button {...args} color="green">Green Button</Button>
				<Button {...args} color="red">Red Button</Button>
				<Button {...args} color="blue">Blue Button</Button>
				<Button {...args} color="gray">Gray Button</Button>
				<Button {...args} color="whiteAlpha">WhiteAlpha Button</Button>
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
			</GridList>
		);
	},
};

export const WithIcon: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args} icon={<PlusOutlined />}>Icon Left</Button>
				<Button {...args} icon={<PlusOutlined />} iconPosition="right">Icon Right</Button>
			</GridList>
		);
	},
};

export const CircleShape: Story = {
	render: function Render(args) {
		return (
			<GridList>
				<Button {...args} icon={<PlusOutlined />} isCircle={true} size="sm" />
				<Button {...args} icon={<PlusOutlined />} isCircle={true} />
				<Button {...args} icon={<PlusOutlined />} isCircle={true} size="lg" />
			</GridList>
		);
	},
};