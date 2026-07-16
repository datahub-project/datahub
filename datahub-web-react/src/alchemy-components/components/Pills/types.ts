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

/**
 * A trailing icon inside a Pill. Prefer this shape over `rightIcon`/`onClickRightIcon`
 * when you need MORE THAN ONE trailing icon (e.g. edit + remove).
 */
export interface PillRightIcon {
    icon: React.ComponentType<any>;
    onClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    ariaLabel?: string;
    testId?: string;
}

export interface PillProps extends Partial<PillPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    label: string;
    color?: ColorOptions;
    rightIcon?: React.ComponentType<any>;
    /**
     * Optional array of trailing icons — use instead of `rightIcon` when you need more
     * than one (e.g. edit + remove on the same pill). Ignored if empty.
     */
    rightIcons?: PillRightIcon[];
    leftIcon?: React.ComponentType<any>;
    customStyle?: React.CSSProperties;
    showLabel?: boolean;
    customIconRenderer?: () => React.ReactNode;
    onClickRightIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onClickLeftIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onPillClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    className?: string;
    dataTestId?: string;
}

export type PillStyleProps = PillPropsDefaults & Pick<PillProps, 'color'>;
