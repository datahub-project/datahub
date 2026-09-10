import type { IconWeight, Icon as PhosphorIcon } from '@phosphor-icons/react';
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

/** Props forwarded to a Pill icon (matches what Alchemy `Icon` passes through). */
export type PillIconProps = {
    style?: React.CSSProperties;
    weight?: IconWeight;
    className?: string;
    color?: string;
    size?: number | string;
};

/** Phosphor icons plus any component that accepts the props we actually pass. */
export type PillIcon = PhosphorIcon | React.ComponentType<PillIconProps>;

/**
 * A trailing icon inside a Pill. Prefer this shape over `rightIcon`/`onClickRightIcon`
 * when you need MORE THAN ONE trailing icon (e.g. edit + remove).
 */
export interface PillRightIcon {
    icon: PillIcon;
    onClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    ariaLabel?: string;
    testId?: string;
}

export interface PillProps extends Partial<PillPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    label: string;
    color?: ColorOptions;
    rightIcon?: PillIcon;
    /**
     * Optional array of trailing icons — use instead of `rightIcon` when you need more
     * than one (e.g. edit + remove on the same pill). Ignored if empty.
     */
    rightIcons?: PillRightIcon[];
    leftIcon?: PillIcon;
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
