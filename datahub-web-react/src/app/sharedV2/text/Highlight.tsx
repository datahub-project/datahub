import React, { CSSProperties, HTMLAttributes, ReactNode } from 'react';

type Props = Omit<HTMLAttributes<HTMLSpanElement>, 'children'> & {
    search?: string | RegExp | null;
    caseSensitive?: boolean;
    matchClass?: string;
    matchStyle?: CSSProperties;
    children?: ReactNode;
};

type TextPart = { text: string; isMatch: boolean };

function toRegExp(search: string | RegExp, caseSensitive: boolean): RegExp {
    // Drop the global flag: a stateful lastIndex would skip matches between exec calls
    if (search instanceof RegExp) return new RegExp(search.source, search.flags.replace('g', ''));
    return new RegExp(search.replace(/[|\\{}()[\]^$+*?.-]/g, '\\$&'), caseSensitive ? '' : 'i');
}

function splitByMatches(text: string, regex: RegExp): TextPart[] {
    const parts: TextPart[] = [];
    let remaining = text;
    while (remaining) {
        const match = regex.exec(remaining);
        if (!match || !match[0]) {
            parts.push({ text: remaining, isMatch: false });
            break;
        }
        if (match.index > 0) parts.push({ text: remaining.slice(0, match.index), isMatch: false });
        parts.push({ text: match[0], isMatch: true });
        remaining = remaining.slice(match.index + match[0].length);
    }
    return parts;
}

/**
 * Wraps every match of `search` in a <mark>. Drop-in replacement for `react-highlighter`,
 * keeping its markup (outer span, plain spans, `mark.highlight`) so existing styles still apply.
 */
export default function Highlight({
    search,
    caseSensitive = false,
    matchClass = 'highlight',
    matchStyle,
    children,
    ...spanProps
}: Props) {
    const isText = typeof children === 'string' || typeof children === 'number';
    if (!isText || !search) return <span {...spanProps}>{children}</span>;

    const parts = splitByMatches(String(children), toRegExp(search, caseSensitive));
    return (
        <span {...spanProps}>
            {parts.map((part, index) =>
                part.isMatch ? (
                    // eslint-disable-next-line react/no-array-index-key
                    <mark key={index} className={matchClass} style={matchStyle}>
                        {part.text}
                    </mark>
                ) : (
                    // eslint-disable-next-line react/no-array-index-key
                    <span key={index}>{part.text}</span>
                ),
            )}
        </span>
    );
}
