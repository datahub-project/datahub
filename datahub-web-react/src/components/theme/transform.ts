import type { RotationOptions } from './config/types';

/*
	Common transform CSS properties
*/

const rotate: Record<RotationOptions, string> = {
    '0': 'rotate(0deg)',
    '90': 'rotate(90deg)',
    '180': 'rotate(180deg)',
    '270': 'rotate(270deg)',
};

export const transform = {
    rotate,
};
