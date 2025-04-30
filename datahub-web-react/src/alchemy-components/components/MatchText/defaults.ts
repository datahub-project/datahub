import { MatchTextProps } from '@components/components/MatchText/types';
import { textDefaults } from '@components/components/Text';

export const matchTextDefaults: Partial<MatchTextProps> = {
    ...textDefaults,
    highlightedTextProps: {
        weight: 'bold',
    },
};
