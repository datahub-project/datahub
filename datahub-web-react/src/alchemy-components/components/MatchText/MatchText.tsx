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

    const textPartsWithKeys = markedTextParts.map((part, index) => ({
        ...part,
        key: `${index}-${part.text}${part.highlighted && '-highlighted'}`,
    }));

    return (
        <Text {...props}>
            {textPartsWithKeys.map((part) => {
                if (part.highlighted)
                    return (
                        <Text {...{ ...props, ...highlightedTextProps }} type="span" key={part.key}>
                            {part.text}
                        </Text>
                    );
                return <React.Fragment key={part.key}>{part.text}</React.Fragment>;
            })}
        </Text>
    );
}
