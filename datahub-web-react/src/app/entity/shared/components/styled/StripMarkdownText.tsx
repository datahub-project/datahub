import React from 'react';
import removeMd from '@tommoor/remove-markdown';
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

export default function NoMarkdownViewer({ children, readMore, suffix, limit, shouldWrap }: Props) {
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
            {plainText} {showReadMore && <>{readMore}</>} {suffix}
        </RemoveMarkdownContainer>
    );
}
