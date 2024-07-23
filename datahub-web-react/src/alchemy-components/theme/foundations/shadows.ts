import { BoxShadowOptions } from '../config';

const shadows: Record<BoxShadowOptions, string> = {
    xs: '0px 1px 2px 0px rgba(0, 0, 0, 0.05)',
    sm: '0 4px 4px 0 rgba(0 0 0 / 0.25)',
    md: '0 8px 8px 4px rgba(0 0 0 / 0.25)',
    lg: '0 12px 12px 8px rgba(0 0 0 / 0.25)',
    xl: '0 16px 16px 12px rgba(0 0 0 / 0.25)',
    '2xl': '0 24px 24px 16px rgba(0 0 0 / 0.25)',
    inner: 'inset 0 2px 4px 0 rgba(0 0 0 / 0.06)',
    outline: '0 0 0 3px rgba(66, 153, 225, 0.5)',
    none: 'none',
    dropdown: '0px 0px 14px 0px #00000026',
};

export default shadows;
