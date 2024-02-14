import React from 'react';
import styled from 'styled-components';

const RemoveMarkdownContainer = styled.div<{ shouldWrap: boolean }>`
    display: block;
    overflow-wrap: break-word;
    white-space: ${(props) => (props.shouldWrap ? 'normal' : 'nowrap')};
    width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
`;

export type Props = {
    children: string | undefined | null;
    readMore?: JSX.Element;
    suffix?: JSX.Element;
    limit?: number;
    shouldWrap?: boolean;
    customRender?: (text: string) => JSX.Element;
};

function extractContentBetweenHeaders(markdown: string): string {
    // Regular expression to match Markdown headers (both # and ## styles)
    const headerRegex = /^#+\s+(.*)$/gm;

    const match = headerRegex.exec(markdown);
    if (!match) {
        return markdown;
    }

    const startIndex = match.index + match[0].length;
    const endIndex = headerRegex.exec(markdown)?.index || markdown.length;
    return markdown.substring(startIndex, endIndex).trim();
}

export const removeMarkdown = (text: string) => {
    return extractContentBetweenHeaders(text);
};

export default function NoMarkdownViewer({ children, customRender, readMore, suffix, limit, shouldWrap }: Props) {
    let plainText = removeMarkdown(children || '');

    if (limit) {
        let abridgedPlainText = plainText.substring(0, limit);
        if (abridgedPlainText.length < plainText.length) {
            abridgedPlainText = `${abridgedPlainText}...`;
        }
        plainText = abridgedPlainText;
    }

    const showReadMore = plainText.length >= (limit || 0);

    return (
        <RemoveMarkdownContainer shouldWrap={!!shouldWrap}>
            {customRender ? customRender(plainText) : plainText}
            {showReadMore && <>{readMore}</>} {suffix}
        </RemoveMarkdownContainer>
    );
}
