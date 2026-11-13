import React, { CSSProperties, HTMLAttributes, ReactNode } from 'react';

type Props = Omit<HTMLAttributes<HTMLSpanElement>, 'children'> & {
    search?: string | RegExp | null;
    caseSensitive?: boolean;
    matchClass?: string;
    matchStyle?: CSSProperties;
    children?: ReactNode;
};

type TextPart = { text: string; isMatch: boolean };

// Always global: matchAll needs it, and scanning the whole text keeps anchors and lookbehinds correct
function toRegExp(search: string | RegExp, caseSensitive: boolean): RegExp {
    if (search instanceof RegExp) {
        return new RegExp(search.source, search.flags.includes('g') ? search.flags : `${search.flags}g`);
    }
    return new RegExp(search.replace(/[|\\{}()[\]^$+*?.-]/g, '\\$&'), caseSensitive ? 'g' : 'gi');
}

function splitByMatches(text: string, regex: RegExp): TextPart[] {
    const parts: TextPart[] = [];
    let last = 0;
    Array.from(text.matchAll(regex)).forEach((match) => {
        const index = match.index ?? 0;
        if (!match[0]) return;
        if (index > last) parts.push({ text: text.slice(last, index), isMatch: false });
        parts.push({ text: match[0], isMatch: true });
        last = index + match[0].length;
    });
    if (last < text.length) parts.push({ text: text.slice(last), isMatch: false });
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
