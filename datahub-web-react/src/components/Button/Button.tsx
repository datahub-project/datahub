import React from 'react';

import { ButtonBase } from './components';
import { buttonDefaults, ButtonProps } from './types';

export const Button = ({
	variant = buttonDefaults.variant,
	color = buttonDefaults.color,
	size = buttonDefaults.size,
	isCircle = buttonDefaults.isCircle,
	icon, // default undefined
	iconPosition = buttonDefaults.iconPosition,
	children,
	...props
}: ButtonProps) => {
	return (
		<ButtonBase
			variant={variant}
			color={color}
			size={size}
			isCircle={isCircle}
			{...props}
		>
			{icon && iconPosition === 'left' && icon}
			{children}
			{icon && iconPosition === 'right' && icon}
		</ButtonBase>
	);
};