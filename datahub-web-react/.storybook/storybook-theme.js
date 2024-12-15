import { create } from '@storybook/theming';
import brandImage from './storybook-logo.svg';

import theme, { typography } from '../src/alchemy-components/theme';

export default create({
	// config 
	base: 'light',
	brandTitle: 'Acryl Design System',
	brandUrl: '/?path=/docs/',
	brandImage: brandImage,
	brandTarget: '_self',

	// styles 
	fontBase: typography.fontFamily,
	fontCode: 'monospace',

	colorPrimary: theme.semanticTokens.colors.primary,
	colorSecondary: theme.semanticTokens.colors.secondary,

	// UI
	appBg: theme.semanticTokens.colors['body-bg'],
	appContentBg: theme.semanticTokens.colors['body-bg'],
	appPreviewBg: theme.semanticTokens.colors['body-bg'],
	appBorderColor: theme.semanticTokens.colors['border-color'],
	appBorderRadius: 4,

	// Text colors
	textColor: theme.semanticTokens.colors['body-text'],
	textInverseColor: theme.semanticTokens.colors['inverse-text'],
	textMutedColor: theme.semanticTokens.colors['subtle-text'],

	// Toolbar default and active colors
	barTextColor: theme.semanticTokens.colors['body-text'],
	barSelectedColor: theme.semanticTokens.colors['subtle-bg'],
	barHoverColor: theme.semanticTokens.colors['subtle-bg'],
	barBg: theme.semanticTokens.colors['body-bg'],

	// Form colors
	inputBg: theme.semanticTokens.colors['body-bg'],
	inputBorder: theme.semanticTokens.colors['border-color'],
	inputTextColor: theme.semanticTokens.colors['body-text'],
	inputBorderRadius: 4,

	// Grid 
	gridCellSize: 6,
});