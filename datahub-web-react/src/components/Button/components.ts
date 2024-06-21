import styled from 'styled-components';

import { text as BaseText, colors, radii, spacing } from '../theme';
import { buttonDefaults, ButtonProps } from './types';

import { getColorValues } from '../theme/utils';

const colorStates = {
	primary: {},
	violet: {
		background: { ...getColorValues({ color: 'violet', shade: 'default' }) },
		border: { ...getColorValues({ color: 'violet', shade: 'default' }) },
		text: { ...getColorValues({ color: 'white' }) },
	},
	gray: {
		background: { ...getColorValues({ color: 'gray', shade: 'default' }) },
		border: { ...getColorValues({ color: 'gray', shade: 'default' }) },
		text: { ...getColorValues({ color: 'white' }) },
	},
	green: {
		background: { ...getColorValues({ color: 'green', shade: 'default' }) },
		border: { ...getColorValues({ color: 'green', shade: 'default' }) },
		text: { ...getColorValues({ color: 'white' }) },
	},
	red: {
		background: { ...getColorValues({ color: 'red', shade: 'default' }) },
		border: { ...getColorValues({ color: 'red', shade: 'default' }) },
		text: { ...getColorValues({ color: 'white' }) },
	},
	blue: {
		background: { ...getColorValues({ color: 'blue', shade: 'default' }) },
		border: { ...getColorValues({ color: 'blue', shade: 'default' }) },
		text: { ...getColorValues({ color: 'white' }) },
	},
	whiteAlpha: {
		background: {
			default: colors.gray[50],
			hover: colors.gray[100],
			active: colors.gray[200],
		},
		border: {
			default: colors.gray[50],
			hover: colors.gray[100],
			active: colors.gray[200],
		},
		text: {
			default: colors.gray[500],
			hover: colors.gray[500],
			active: colors.gray[500],
		},
	},
};

// Define primary color
colorStates.primary = colorStates.violet;

// Filled Styles
const filled = (color) => `
	background-color: ${color.background.default};
	border: 1px solid ${color.border.default};
	color: ${color.text.default};

	&:hover {
		background-color: ${color.background.hover};
		border-color: ${color.border.hover};
		color: ${color.text.hover};
	}

	&:active,
	&:focus {
		background-color: ${color.background.active};
		border-color: ${color.border.active};
		color: ${color.text.active};
	}

	&:disabled {
		background-color: ${colors.gray[100]};
		color: ${colors.gray[300]};
		border-color: ${colors.gray[300]};
	}
`;

// Outline Styles
const outline = (color, isAlpha) => `
	background-color: transparent;
	border: 1px solid ${color.border.default};
	color: ${isAlpha ? color.text.default : color.border.default};

	&:hover,
	&:focus {
		background-color: ${color.background.hover};
		border-color: ${color.border.hover};
		color: ${color.text.hover};
	}

	&:active {
		background-color: ${color.background.active};
		border-color: ${color.border.active};
		color: ${color.border.active};
	}

	&:disabled {
		background-color: transparent;
		color: ${colors.gray[300]};
		border-color: ${colors.gray[300]};
	}
`;

// Text Styles
const text = (color, isAlpha) => `
	background-color: transparent;
	border: none;
	color: ${isAlpha ? color.text.default : color.background.default};

	&:hover,
	&:active,
	&:focus {
		background-color: transparent;
		color: ${isAlpha ? color.text.hover : color.background.hover};
	}

	&:disabled {
		background-color: transparent;
		color: ${colors.gray[300]};
	}
`;

// Size Styles
const getSize = (size) => {
	switch (size) {
		case 'sm':
			return `
				font-size: ${BaseText.size.sm};
				padding: ${spacing.xxsm} ${spacing.xsm};
				border-radius: ${radii.sm};
			`;
		case 'md':
			return `
				font-size: ${BaseText.size.md};
				padding: ${spacing.xsm} ${spacing.sm};
				border-radius: ${radii.md};
			`;
		case 'lg':
			return `
				font-size: ${BaseText.size.lg};
				padding: ${spacing.md} ${spacing.lg};
				border-radius: ${radii.lg};
			`;
		default: // default is 'md'
			return `
				font-size: ${BaseText.size.md};
				padding: ${spacing.xsm} ${spacing.sm};
				border-radius: ${radii.md};
			`;
	}
};


export const ButtonBase = styled.button<{
	variant?: ButtonProps['variant'];
	color?: ButtonProps['color'];
	size?: ButtonProps['size'];
	isCircle?: boolean;
}>`
	display: flex;
	align-items: center;
	gap: ${spacing.xsm};

	${({ variant, color }) => {
		const selectedColor = colorStates[color || buttonDefaults.color || ''];
		const isAlpha = color?.includes('Alpha');

		switch (variant) {
			case 'filled':
				return filled(selectedColor);
			case 'outline':
				return outline(selectedColor, isAlpha);
			case 'text':
				return text(selectedColor, isAlpha);
			default: // default is 'filled'
				return filled(buttonDefaults.color);
		}
	}}

	${({ size }) => getSize(size || buttonDefaults.size)}

	font-family: ${BaseText.family.default};
	font-weight: ${BaseText.weight.normal};
	line-height: ${BaseText.lineHeight.normal};

	${({ isCircle }) => isCircle && `
		border-radius: 50%;
		padding: ${spacing.xsm};
	`}

	transition: 
		background-color 0.15s, 
		border-color 0.15s, 
		color 0.15s;

	cursor: pointer;

	&:disabled {
		cursor: not-allowed;
	}
`;