/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import { matchTextDefaults } from '@components/components/MatchText/defaults';
import { MatchTextProps } from '@components/components/MatchText/types';
import { annotateHighlightedText } from '@components/components/MatchText/utils';
import { Text, textDefaults } from '@components/components/Text';

export default function MatchText({
    text,
    highlight,
    highlightedTextProps = matchTextDefaults.highlightedTextProps,
    type = textDefaults.type,
    color = textDefaults.color,
    size = textDefaults.size,
    weight = textDefaults.weight,
    ...props
}: MatchTextProps) {
    const textProps = useMemo(() => ({ ...props, type, color, size, weight }), [type, color, size, weight, props]);

    const markedTextParts = annotateHighlightedText(text, highlight);

    const textPartsWithKeys = markedTextParts.map((part, index) => ({
        ...part,
        key: `${index}-${part.text}${part.highlighted && '-highlighted'}`,
    }));

    return (
        <Text {...textProps}>
            {textPartsWithKeys.map((part) => {
                if (part.highlighted)
                    return (
                        <Text {...{ ...textProps, ...highlightedTextProps }} type="span" key={part.key}>
                            {part.text}
                        </Text>
                    );
                return <React.Fragment key={part.key}>{part.text}</React.Fragment>;
            })}
        </Text>
    );
}
