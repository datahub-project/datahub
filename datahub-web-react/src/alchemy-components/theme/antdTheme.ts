import { type ThemeConfig, theme as antdThemeApi } from 'antd';

import type { Theme } from '@conf/theme/types';

const INPUT_BORDER_COLOR = '#d9d9d9';

/**
 * antd 5 replaced Less variables with design tokens. These reproduce the antd 4 Less variables the app
 * used to set. Where antd 5 derives a token differently from antd 4 (heading sizes, line height, radii),
 * it is pinned to the antd 4 value so layouts don't shift.
 */
export const antdTheme: ThemeConfig = {
    token: {
        colorPrimary: '#533FD1',
        colorLink: '#533FD1',
        colorText: 'rgba(0, 0, 0, 0.85)',
        colorTextHeading: 'rgba(0, 0, 0, 0.85)',
        colorTextSecondary: 'rgba(0, 0, 0, 0.45)',
        colorTextDisabled: 'rgba(0, 0, 0, 0.25)',
        colorBorder: '#ececec',
        colorSplit: 'rgba(0, 0, 0, 0.06)',
        colorFillAlter: '#fafafa',
        colorBgLayout: '#ffffff',
        colorHighlight: '#ece9f8',

        // antd 4 left font-family to inheritance; antd 5 sets it on every component root, which would
        // override the V2 layout's Mulish. The body font stack is set in global-overrides.less.
        fontFamily: 'inherit',
        // Typography's code, kbd and pre had no font of their own in antd 4, so they kept Mulish
        fontFamilyCode: 'inherit',
        fontSize: 12,
        fontSizeSM: 10,
        fontSizeHeading1: 33,
        fontSizeHeading2: 26,
        fontSizeHeading3: 21,
        fontSizeHeading4: 18,
        fontSizeHeading5: 14,
        lineHeight: 1.5715,

        borderRadius: 5,
        borderRadiusSM: 5,
        borderRadiusLG: 5,
    },
    components: {
        Button: {
            fontWeight: 600,
            defaultBorderColor: INPUT_BORDER_COLOR,
            defaultShadow: '0px 0px 4px 0px #0000001a',
        },
        Input: { colorBorder: INPUT_BORDER_COLOR },
        InputNumber: { colorBorder: INPUT_BORDER_COLOR },
        // antd 4's fixed option padding and line; antd 5 pads to the control height, adding ~3px a row
        Select: {
            colorBorder: INPUT_BORDER_COLOR,
            optionPadding: '5px 12px',
            optionLineHeight: '22px',
        },
        // Menu item line height is in global-overrides.less; a token would also change custom content
        Dropdown: { paddingBlock: 5 },
        DatePicker: { colorBorder: INPUT_BORDER_COLOR },
        // antd 4's heading line heights, scoped to Typography so Modal and Result keep theirs
        Typography: {
            lineHeightHeading1: 1.23,
            lineHeightHeading2: 1.35,
            lineHeightHeading3: 1.35,
            lineHeightHeading4: 1.4,
            lineHeightHeading5: 1.5,
        },
        // Modal padding isn't tokenized until antd 6; see global-overrides.less
        Modal: {
            titleFontSize: 14,
            titleLineHeight: 22 / 14,
        },
        Layout: {
            headerBg: '#ffffff',
            headerColor: '#434343',
            headerHeight: 60,
            headerPadding: '0 20px',
            bodyBg: '#ffffff',
        },
        Tag: {
            defaultBg: 'transparent',
            defaultColor: '#434343',
        },
    },
};

// GlobalThemeStyles sets these colors, but antd 5's more specific rules win, so pass them as tokens
function getThemedComponents(colors: Theme['colors']): ThemeConfig['components'] {
    return {
        Checkbox: { colorBorder: colors.borderCheckbox },
        Divider: { colorSplit: colors.border },
        Form: { labelColor: colors.text },
        Progress: { defaultColor: colors.buttonFillBrand, remainingColor: colors.bgSurfaceDisabled },
        Tabs: { itemColor: colors.textSecondary },
        Timeline: { tailColor: colors.border },
    };
}

// Dark mode also maps the app's colors onto antd tokens; the dark algorithm derives fills and hovers
export function getAntdTheme(theme: Theme | undefined): ThemeConfig {
    if (!theme) return antdTheme;
    if (theme.id !== 'themeV2Dark') {
        return { ...antdTheme, components: { ...antdTheme.components, ...getThemedComponents(theme.colors) } };
    }
    const { colors } = theme;
    return {
        algorithm: antdThemeApi.darkAlgorithm,
        token: {
            ...antdTheme.token,
            colorPrimary: colors.buttonFillBrand,
            colorLink: colors.hyperlinks,
            colorText: colors.text,
            colorTextHeading: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorTextTertiary: colors.textTertiary,
            colorTextDescription: colors.textSecondary,
            colorTextDisabled: colors.textDisabled,
            colorTextPlaceholder: colors.textPlaceholder,
            colorIcon: colors.icon,
            colorIconHover: colors.iconHover,
            colorBgBase: colors.bg,
            colorBgContainer: colors.bgInput,
            colorBgElevated: colors.bg,
            colorBgLayout: colors.bg,
            colorBgSpotlight: colors.bgTooltip,
            colorBgMask: colors.overlayHeavy,
            colorBgContainerDisabled: colors.bgInputDisabled,
            colorBorder: colors.border,
            colorBorderSecondary: colors.border,
            colorSplit: colors.border,
            colorFillAlter: colors.bgSurface,
            colorHighlight: colors.bgSelectedSubtle,
            controlItemBgHover: colors.bgHover,
            controlItemBgActive: colors.bgSelectedSubtle,
        },
        components: {
            ...antdTheme.components,
            ...getThemedComponents(colors),
            Button: { ...antdTheme.components?.Button, defaultBorderColor: colors.borderInput, defaultShadow: 'none' },
            Input: { colorBorder: colors.borderInput },
            InputNumber: { colorBorder: colors.borderInput },
            Select: { ...antdTheme.components?.Select, colorBorder: colors.borderInput },
            DatePicker: { colorBorder: colors.borderInput },
            Layout: {
                ...antdTheme.components?.Layout,
                headerBg: colors.bg,
                headerColor: colors.text,
                bodyBg: colors.bg,
            },
            Tag: { ...antdTheme.components?.Tag, defaultColor: colors.text },
        },
    };
}
