import { textDefaults } from '../Text';
import { MatchTextProps } from './types';

export const matchTextDefaults: Partial<MatchTextProps> = {
    ...textDefaults,
    highlightedTextProps: {
        weight: 'bold',
    },
};
