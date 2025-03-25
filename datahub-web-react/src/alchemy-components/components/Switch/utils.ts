import { SizeOptions } from '@components/theme/config';

const sliderSize = {
    sm: '14px',
    md: '16px',
    lg: '18px',
    xl: '20px',
};

const inputSize = {
    sm: '35px',
    md: '40px',
    lg: '45px',
    xl: '50px',
};

const translateSize = {
    sm: '22px',
    md: '24px',
    lg: '26px',
    xl: '28px',
};

const iconTransformPositionLeft = {
    sm: {
        checked: '5.5px',
        unchecked: '-16.5px',
    },
    md: {
        checked: '5px',
        unchecked: '-19px',
    },
    lg: {
        checked: '4.5px',
        unchecked: '-21.5px',
    },
    xl: {
        checked: '4px',
        unchecked: '-24px',
    },
};

const iconTransformPositionTop = {
    sm: '-6px',
    md: '-7px',
    lg: '-8px',
    xl: '-9px',
};

export const getToggleSize = (size: SizeOptions, mode: 'slider' | 'input'): string => {
    if (size === 'sm') return mode === 'slider' ? sliderSize.sm : inputSize.sm;
    if (size === 'md') return mode === 'slider' ? sliderSize.md : inputSize.md;
    if (size === 'lg') return mode === 'slider' ? sliderSize.lg : inputSize.lg;
    return mode === 'slider' ? sliderSize.xl : inputSize.xl; // xl
};

export const getInputHeight = (size: SizeOptions) => {
    if (size === 'sm') return sliderSize.sm;
    if (size === 'md') return sliderSize.md;
    if (size === 'lg') return sliderSize.lg;
    return sliderSize.xl; // xl
};

export const getSliderTransformPosition = (size: SizeOptions): string => {
    if (size === 'sm') return `translate(${translateSize.sm}, -50%)`;
    if (size === 'md') return `translate(${translateSize.md}, -50%)`;
    if (size === 'lg') return `translate(${translateSize.lg}, -50%)`;
    return `translate(${translateSize.xl}, -50%)`; // xl
};

export const getIconTransformPositionLeft = (size: SizeOptions, checked: boolean): string => {
    if (size === 'sm') {
        if (checked) return iconTransformPositionLeft.sm.checked;
        return iconTransformPositionLeft.sm.unchecked;
    }

    if (size === 'md') {
        if (checked) return iconTransformPositionLeft.md.checked;
        return iconTransformPositionLeft.md.unchecked;
    }

    if (size === 'lg') {
        if (checked) return iconTransformPositionLeft.lg.checked;
        return iconTransformPositionLeft.lg.unchecked;
    }

    // xl
    if (checked) return iconTransformPositionLeft.xl.checked;
    return iconTransformPositionLeft.xl.unchecked;
};

export const getIconTransformPositionTop = (size: SizeOptions): string => {
    if (size === 'sm') return iconTransformPositionTop.sm;
    if (size === 'md') return iconTransformPositionTop.md;
    if (size === 'lg') return iconTransformPositionTop.lg;
    return iconTransformPositionTop.xl; // xl
};
