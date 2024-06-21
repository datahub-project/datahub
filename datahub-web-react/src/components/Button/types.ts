import { ReactElement, ButtonHTMLAttributes } from 'react';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
	variant?: 'filled' | 'outline' | 'text';
	// corresponds to the "colorStates" in the design system
	color?: 'violet' | 'green' | 'red' | 'blue' | 'gray' | 'whiteAlpha';
	size?: 'sm' | 'md' | 'lg';
	isCircle?: boolean;
	icon?: ReactElement;
	iconPosition?: 'left' | 'right';
}

export const buttonDefaults = {
	variant: 'filled' as ButtonProps['variant'],
	color: 'violet' as ButtonProps['color'],
	size: 'md' as ButtonProps['size'],
	isCircle: false,
	iconPosition: 'left' as ButtonProps['iconPosition'],
};