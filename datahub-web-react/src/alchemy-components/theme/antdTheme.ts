import type { ThemeConfig } from 'antd';

const FONT_FAMILY = [
    'Manrope',
    '-apple-system',
    'BlinkMacSystemFont',
    "'Segoe UI'",
    'Roboto',
    "'Helvetica Neue'",
    'Arial',
    "'Noto Sans'",
    'sans-serif',
    "'Apple Color Emoji'",
    "'Segoe UI Emoji'",
    "'Segoe UI Symbol'",
    "'Noto Color Emoji'",
].join(', ');

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

        fontFamily: FONT_FAMILY,
        fontFamilyCode: "'Roboto Mono', Consolas, 'Liberation Mono', Menlo, Courier, monospace",
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
        Select: { colorBorder: INPUT_BORDER_COLOR },
        DatePicker: { colorBorder: INPUT_BORDER_COLOR },
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
