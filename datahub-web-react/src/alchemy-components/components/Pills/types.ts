import { ColorOptions, SizeOptions, PillVariantOptions } from '@src/alchemy-components/theme/config';
import { HTMLAttributes } from 'react';

export interface PillPropsDefaults {
    variant: PillVariantOptions;
    size: SizeOptions;
    color: ColorOptions;
    clickable: boolean;
}

export interface PillProps extends Partial<PillPropsDefaults>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    label: string;
    color?: ColorOptions;
    rightIcon?: string;
    leftIcon?: string;
    customStyle?: React.CSSProperties;
    customIconRenderer?: () => void;
    onClickRightIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onClickLeftIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onPillClick?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
}

export type PillStyleProps = PillPropsDefaults & Pick<PillProps, 'color'>;
