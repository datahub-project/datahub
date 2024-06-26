import { create } from '@storybook/theming/create';

import { tokens, text } from '../../src/components/theme';

export default create({
	// config 
	base: 'light',
	brandTitle: 'Acryl Design System',
	brandUrl: '/?path=/docs/',
	brandImage: 'https://datahubproject.io/img/acryl-logo-transparent-mark.svg',
	brandTarget: '_self',

	// styles 
	fontBase: `'${text.family.default}', sans-serif`,
	fontCode: 'monospace',

	colorPrimary: tokens.colors.primary.default,
	colorSecondary: tokens.colors.secondary.default,

	// UI
	appBg: tokens.colors.background.white,
	appContentBg: tokens.colors.background.white,
	appPreviewBg: tokens.colors.background.white,
	appBorderColor: tokens.colors.borders.light,
	appBorderRadius: 4,

	// Text colors
	textColor: tokens.colors.text.default,
	textInverseColor: tokens.colors.background.white,
	textMutedColor: tokens.colors.text.default,

	// Toolbar default and active colors
	barTextColor: tokens.colors.text.dark,
	barSelectedColor: tokens.colors.background.mediumGray,
	barHoverColor: tokens.colors.background.mediumGray,
	barBg: tokens.colors.background.white,

	// Form colors
	inputBg: tokens.colors.background.white,
	inputBorder: tokens.colors.borders.light,
	inputTextColor: tokens.colors.text.dark,
	inputBorderRadius: 4,

	// Grid 
	gridCellSize: 6,
});