import React from 'react';
import { Text } from '../Text';
import { matchTextDefaults } from './defaults';
import { MatchTextProps } from './types';
import { annotateHighlightedText } from './utils';

export default function MatchText({
    text,
    highlight,
    highlightedTextProps = matchTextDefaults.highlightedTextProps,
    ...props
}: MatchTextProps) {
    const markedTextParts = annotateHighlightedText(text, highlight);

    return (
        <Text {...props}>
            {markedTextParts.map((part) => {
                if (part.highlighted)
                    return (
                        <Text {...{ ...props, ...highlightedTextProps }} type="span">
                            {part.text}
                        </Text>
                    );
                return part.text;
            })}
        </Text>
    );
}
