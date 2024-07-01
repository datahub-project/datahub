import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { Heading, headingDefaults } from '../../../src/components';

import { VerticalFlexGrid } from '../../docLayoutComponents/DocsComponents';

// More on how to set up stories at: https://storybook.js.org/docs/writing-stories#default-export
const meta = {
	title: 'Typography/Heading',
	component: Heading,
	tags: ['autodocs'],
	// Display Properties 
	parameters: {
		layout: 'centered',
		docs: {
			subtitle: 'Used to render semantic HTML heading elements.',
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
			description: 'The type of heading to display.',
			table: {
				defaultValue: { summary: headingDefaults.type }
			},
		},
		size: {
			description: 'Override the size of the heading.',
			table: {
				defaultValue: { summary: headingDefaults.size }
			},
		},
		color: {
			description: 'Override the color of the heading.',
			table: {
				defaultValue: { summary: headingDefaults.color }
			},
		},
		weight: {
			description: 'Override the weight of the heading.',
			table: {
				defaultValue: { summary: headingDefaults.weight }
			},
		},
	},
	// Define default args
	args: {
		children: 'The content to display within the heading.',
		type: headingDefaults.type,
		size: headingDefaults.size,
		color: headingDefaults.color,
		weight: headingDefaults.weight,
	},
} satisfies Meta<typeof Heading>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
	tags: ['isHidden'],
	render: function Render(args) {
		return (
			<Heading>{args.children}</Heading>
		);
	},
};

export const Sizes: Story = {
	render: function Render(args) {
		return (
			<VerticalFlexGrid>
				<Heading type="h1">H1 {args.children}</Heading>
				<Heading type="h2">H2 {args.children}</Heading>
				<Heading type="h3">H3 {args.children}</Heading>
				<Heading type="h4">H4 {args.children}</Heading>
				<Heading type="h5">H5 {args.children}</Heading>
				<Heading type="h6">H6 {args.children}</Heading>
			</VerticalFlexGrid>
		);
	},
};

export const WithLink: Story = {
	render: function Render(_) {
		return (
			<Heading>
				<a href="#">The content to display within the heading</a>
			</Heading>
		);
	},
};