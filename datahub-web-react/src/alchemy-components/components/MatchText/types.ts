import { TextProps } from '@components/components/Text';

export type TextPropsWithoutChildren = Omit<TextProps, 'children'>;

export interface MatchTextProps extends TextPropsWithoutChildren {
    text: string;
    highlight: string;
    highlightedTextProps?: Partial<TextPropsWithoutChildren>;
}
