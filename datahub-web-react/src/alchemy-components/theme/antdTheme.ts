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
        // Only Typography's code, kbd and pre use this. antd 4 gave them no font of their own, so the
        // V2 layout's Mulish applied; a monospace stack here would override it.
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
        // antd 5 pads options and menu items to fill the control height around the text; antd 4 used a
        // fixed 5px padding and 22px line, so taller custom option content grew by ~3px a row
        Select: {
            colorBorder: INPUT_BORDER_COLOR,
            optionPadding: '5px 12px',
            optionLineHeight: '22px',
        },
        // The menu items' 22px line is set in global-overrides.less: a lineHeight token here would also
        // change the line height of custom dropdown content
        Dropdown: { paddingBlock: 5 },
        DatePicker: { colorBorder: INPUT_BORDER_COLOR },
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

/**
 * Light mode keeps the antd 4 values above. antd 5 paints its components from these tokens at runtime,
 * at a specificity GlobalThemeStyles can't always beat, so dark mode has to reach antd through its
 * tokens too: the dark algorithm derives fills and hovers, and the app's own colors pin the rest.
 */
export function getAntdTheme(theme: Theme | undefined): ThemeConfig {
    if (!theme || theme.id !== 'themeV2Dark') return antdTheme;
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
            Button: { ...antdTheme.components?.Button, defaultBorderColor: colors.borderInput, defaultShadow: 'none' },
            Input: { colorBorder: colors.borderInput },
            InputNumber: { colorBorder: colors.borderInput },
            Select: { ...antdTheme.components?.Select, colorBorder: colors.borderInput },
            DatePicker: { colorBorder: colors.borderInput },
            Checkbox: { colorBorder: colors.borderCheckbox },
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
