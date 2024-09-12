import { HTMLAttributes } from 'react';
import { ColorOptions, SizeOptions, VariantOptions } from '@src/alchemy-components/theme/config';

export interface PillStyleProps {
    colorScheme?: ColorOptions; // need to keep colorScheme because HTMLAttributes also have color property
    variant?: VariantOptions;
    size?: SizeOptions;
}

export interface PillProps extends HTMLAttributes<HTMLElement>, PillStyleProps {
    label: string;
    rightIcon?: string;
    leftIcon?: string;
    onClickRightIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
    onClickLeftIcon?: (e: React.MouseEvent<HTMLElement, MouseEvent>) => void;
}
