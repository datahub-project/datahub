import React from 'react';
import Highlight from 'react-highlighter';
import removeMd from '@tommoor/remove-markdown';
import styled from 'styled-components';
import { useHighlightedValue } from '../../../../search/highlight/HighlightContext';

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
    highlightField?: string;
};

export const removeMarkdown = (text: string) => {
    return removeMd(text, {
        stripListLeaders: true,
        gfm: true,
        useImgAltText: true,
    })
        .replace(/\n*\n/g, ' • ') // replace linebreaks with •
        .replace(/^•/, ''); // remove first •
};

export default function NoMarkdownViewer({ children, readMore, suffix, limit, shouldWrap, highlightField }: Props) {
    let plainText = removeMarkdown(children || '');
    const highlightedValue = useHighlightedValue(highlightField);

    if (limit) {
        let abridgedPlainText = plainText.substring(0, limit);
        if (abridgedPlainText.length < plainText.length) {
            abridgedPlainText = `${abridgedPlainText}...`;
        }
        plainText = abridgedPlainText;
    }

    const showReadMore = plainText.length >= (limit || 0);
    // todo - pass search query on through here through the search context?
    // that way we can check if it should highlight this stuff
    console.log({ highlightedValue });

    return (
        <RemoveMarkdownContainer shouldWrap={!!shouldWrap}>
            {highlightedValue ? <Highlight search={highlightedValue}>{plainText}</Highlight> : plainText}
            {showReadMore && <>{readMore}</>} {suffix}
        </RemoveMarkdownContainer>
    );
}
