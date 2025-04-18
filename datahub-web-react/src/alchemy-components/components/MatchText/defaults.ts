<<<<<<< HEAD
import { MatchTextProps } from '@components/components/MatchText/types';
import { textDefaults } from '@components/components/Text';
=======
import { textDefaults } from '../Text';
import { MatchTextProps } from './types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export const matchTextDefaults: Partial<MatchTextProps> = {
    ...textDefaults,
    highlightedTextProps: {
        weight: 'bold',
    },
};
