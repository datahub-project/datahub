import React, { HTMLAttributes } from 'react';

import { ColorOptions, PillVariantOptions, SizeOptions } from '@src/alchemy-components/theme/config';
import { Theme } from '@src/conf/theme/types';

export interface PillPropsDefaults {
    variant: PillVariantOptions;
    size: SizeOptions;
    color: ColorOptions;
    clickable: boolean;
    theme?: Theme;
}

export interface PillProps extends Partial<PillPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    label: string;
    color?: ColorOptions;
    rightIcon?: React.ComponentType<any>;
    leftIcon?: React.ComponentType<any>;
    customStyle?: React.CSSProperties;
    showLabel?: boolean;
    customIconRenderer?: () => void;
    onClickRightIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onClickLeftIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onPillClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    className?: string;
    dataTestId?: string;
}

export type PillStyleProps = PillPropsDefaults & Pick<PillProps, 'color'>;
