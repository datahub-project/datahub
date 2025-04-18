<<<<<<< HEAD
import { TextProps } from '@components/components/Text';
=======
import { TextProps } from '../Text';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export type TextPropsWithoutChildren = Omit<TextProps, 'children'>;

export interface MatchTextProps extends TextPropsWithoutChildren {
    text: string;
    highlight: string;
    highlightedTextProps?: Partial<TextPropsWithoutChildren>;
}
