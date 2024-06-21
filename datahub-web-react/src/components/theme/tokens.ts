/*
	Semantic Tokens for standardizing color values for common use cases
*/

import { colors as BaseColors } from './colors';
import { text as BaseText } from './text';

import {
	BACKGROUND_LIGHT_VALUE,
	BACKGROUND_DEFAULT_VALUE,
	BACKGROUND_DARK_VALUE,
	TEXT_LIGHT_VALUE,
	TEXT_DEFAULT_VALUE,
	TEXT_DARK_VALUE,
	BORDER_LIGHT_VALUE,
	BORDER_DEFAULT_VALUE,
	BORDER_DARK_VALUE,
} from './config';

// Define the color tokens
const colors = {
	primary: {
		default: BaseColors.violet[500],
		light: BaseColors.violet[300],
		dark: BaseColors.violet[700],
	},
	error: BaseColors.red[500],
	success: BaseColors.green[500],
	warning: BaseColors.yellow[500],
	info: BaseColors.blue[500],
	text: {
		default: BaseColors.gray[TEXT_DEFAULT_VALUE],
		light: BaseColors.gray[TEXT_LIGHT_VALUE],
		dark: BaseColors.gray[TEXT_DARK_VALUE],
	},
	background: {
		white: BaseColors.white,
		lightGray: BaseColors.gray[BACKGROUND_LIGHT_VALUE],
		mediumGray: BaseColors.gray[BACKGROUND_DEFAULT_VALUE],
		darkGray: BaseColors.gray[BACKGROUND_DARK_VALUE],
	},
	borders: {
		default: BaseColors.gray[BORDER_DEFAULT_VALUE],
		light: BaseColors.gray[BORDER_LIGHT_VALUE],
		dark: BaseColors.gray[BORDER_DARK_VALUE],
	}
};

// Define the shadow tokens
const shadows = {
	quickDropdown: '0 0 24px 0 rgba(0 0 0 / 0.1)',
	elevationButton: '0 0 8px 4px rgba(0 0 0 / 0.36)',
	elevationDialogeModal: '0 4px 8px 3px rgba(0 0 0 / 0.15), 0 4px 4px 0 rgba(0 0 0 / 0.25)',
}

const text = {
	weight: {
		heading: BaseText.weight.bold,
		subheading: BaseText.weight.normal,
		body: BaseText.weight.normal,
		caption: BaseText.weight.light,
	}
}

export const tokens = {
	colors,
	shadows,
	text
};