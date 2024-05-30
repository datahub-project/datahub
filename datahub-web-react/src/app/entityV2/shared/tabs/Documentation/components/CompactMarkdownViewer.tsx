import React, { useState, useCallback } from 'react';
import { Button } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../constants';
import { Editor } from './editor/Editor';

const LINE_HEIGHT = 1.5;

const MarkdownContainer = styled.div`
    max-width: 100%;
    position: relative;
`;

const EllipsisWrapper = styled.span`
    font-size: 150%;
    line-height: 0.5em;
`;

const ShowMoreWrapper = styled.div`
    align-items: start;
    display: flex;
    flex-direction: column;
`;

const CustomButton = styled(Button)`
    padding: 0;
    color: ${REDESIGN_COLORS.ACTION_ICON_GREY};
`;

const MarkdownViewContainer = styled.div`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    overflow-x: hidden;
    overflow-y: hidden;
`;

const CompactEditor = styled(Editor)<{ limit: number | null }>`
    .remirror-editor.ProseMirror {
        ${({ limit }) => limit && `max-height: ${limit * LINE_HEIGHT}em;`}
        h1 {
            font-size: 1.4em;
        }

        h2 {
            font-size: 1.3em;
        }

        h3 {
            font-size: 1.2em;
        }

        h4 {
            font-size: 1.1em;
        }

        h5,
        h6 {
            font-size: 1em;
        }

        p {
            margin-bottom: 0;
        }

        padding: 0;
    }
`;

const FixedLineHeightEditor = styled(CompactEditor)`
    .remirror-editor.ProseMirror {
        * {
            line-height: ${LINE_HEIGHT};
            font-size: 1em !important;
            margin-top: 0;
            margin-bottom: 0;
        }
    }
`;

export type Props = {
    content: string;
    lineLimit?: number | null;
    fixedLineHeight?: boolean;
};

export default function CompactMarkdownViewer({ content, lineLimit = 4, fixedLineHeight = false }: Props) {
    const [isShowingMore, setIsShowingMore] = useState(false);
    const [isTruncated, setIsTruncated] = useState(false);

    const measuredRef = useCallback((node: HTMLDivElement | null) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setIsTruncated(node.scrollHeight > node.clientHeight);
            });
            resizeObserver.observe(node);
        }
    }, []);

    const StyledEditor = fixedLineHeight ? FixedLineHeightEditor : CompactEditor;

    return (
        <MarkdownContainer>
            <MarkdownViewContainer ref={measuredRef}>
                <StyledEditor limit={isShowingMore ? null : lineLimit} content={content} readOnly />
            </MarkdownViewContainer>
            {(isShowingMore || isTruncated) && ( // "show more" when isTruncated, "show less" when isShowingMore
                <ShowMoreWrapper>
                    {isTruncated && <EllipsisWrapper>...</EllipsisWrapper>}
                    <CustomButton type="link" onClick={() => setIsShowingMore(!isShowingMore)}>
                        {isShowingMore ? 'show less' : 'show more'}
                    </CustomButton>
                </ShowMoreWrapper>
            )}
        </MarkdownContainer>
    );
}
