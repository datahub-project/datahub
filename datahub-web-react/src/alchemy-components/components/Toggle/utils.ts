import { SizeOptions } from '@components/theme/config';

export const getToggleSize = (size: SizeOptions, mode: 'slider' | 'input'): string => {
    if (size === 'sm') {
        return mode === 'slider' ? '18px' : '45px';
    }
    if (size === 'md') {
        return mode === 'slider' ? '22px' : '50px';
    }
    if (size === 'lg') {
        return mode === 'slider' ? '25px' : '55px';
    }
    return mode === 'slider' ? '28px' : '60px';
};

export const getInputHeight = (size: SizeOptions) => {
    if (size === 'sm') return '18px';
    if (size === 'md') return '22px';
    if (size === 'lg') return '25px';
    return '28px';
};

export const getSliderTransformPosition = (size: SizeOptions): string => {
    if (size === 'sm' || size === 'md') {
        return 'translate(30px, -50%)';
    }
    if (size === 'lg') {
        return 'translate(33px, -50%)';
    }
    return 'translate(35px, -50%)';
};

export const getIconTransformPositionLeft = (size: SizeOptions, checked: boolean): string => {
    if (size === 'sm') {
        if (checked) {
            return '9px';
        }
        return '-21.5px';
    }
    if (size === 'md') {
        if (checked) {
            return '7px';
        }
        return '-23px';
    }
    if (size === 'lg') {
        if (checked) {
            return '8px';
        }
        return '-25px';
    }
    if (size === 'xl') {
        if (checked) {
            return '8px';
        }
        return '-27px';
    }
    return checked ? '7px' : '-23px';
};

export const getIconTransformPositionTop = (size: SizeOptions): string => {
    if (size === 'sm') {
        return '-5.5px';
    }
    if (size === 'md') {
        return '-7px';
    }
    if (size === 'lg') {
        return '-8px';
    }
    if (size === 'xl') {
        return '-9px';
    }
    return '-7px';
};
