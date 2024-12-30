import { Button } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';
import { Editor } from './editor/Editor';

const LINE_HEIGHT = 1.5;

const ShowMoreWrapper = styled.div`
    align-items: start;
    display: flex;
    flex-direction: column;
`;

const MarkdownContainer = styled.div<{ lineLimit?: number | null }>`
    max-width: 100%;
    position: relative;
    ${(props) =>
        props.lineLimit &&
        props.lineLimit <= 1 &&
        ` 
        display: flex;
        align-items: center;
        gap: 4px;
        ${ShowMoreWrapper}{
            flex-direction: row;
            align-items: center;
            gap: 4px;
        }
    `}
`;

const CustomButton = styled(Button)`
    padding: 0;
    color: #676b75;
`;

const MarkdownViewContainer = styled.div<{ scrollableY: boolean }>`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    overflow-x: hidden;
    overflow-y: ${(props) => (props.scrollableY ? 'auto' : 'hidden')};
`;

const CompactEditor = styled(Editor)<{ limit: number | null; customStyle?: React.CSSProperties }>`
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
            ${(props) => props?.customStyle?.fontSize && `font-size: ${props?.customStyle?.fontSize}`};
            margin-bottom: 0;
        }

        padding: 0;
    }
`;

const FixedLineHeightEditor = styled(CompactEditor)<{ customStyle?: React.CSSProperties }>`
    .remirror-editor.ProseMirror {
        * {
            line-height: ${LINE_HEIGHT};
            font-size: 1em !important;
            margin-top: 0;
            margin-bottom: 0;
        }
        p {
            font-size: ${(props) => (props?.customStyle?.fontSize ? props?.customStyle?.fontSize : '1em')} !important;
        }
    }
`;

export type Props = {
    content: string;
    lineLimit?: number | null;
    fixedLineHeight?: boolean;
    isShowMoreEnabled?: boolean;
    customStyle?: React.CSSProperties;
    scrollableY?: boolean; // Whether the viewer is vertically scrollable.
    handleShowMore?: () => void;
    hideShowMore?: boolean;
};

export default function CompactMarkdownViewer({
    content,
    lineLimit = 4,
    fixedLineHeight = false,
    isShowMoreEnabled = false,
    customStyle = {},
    scrollableY = true,
    handleShowMore,
    hideShowMore,
}: Props) {
    const [isShowingMore, setIsShowingMore] = useState(false);
    const [isTruncated, setIsTruncated] = useState(false);

    useEffect(() => {
        if (isShowMoreEnabled) {
            setIsShowingMore(isShowMoreEnabled);
        }
        return () => {
            setIsShowingMore(false);
        };
    }, [isShowMoreEnabled]);

    const measuredRef = useCallback((node: HTMLDivElement | null) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setIsTruncated(node.scrollHeight > node.clientHeight + 1);
            });
            resizeObserver.observe(node);
        }
    }, []);

    const StyledEditor = fixedLineHeight ? FixedLineHeightEditor : CompactEditor;

    return (
        <MarkdownContainer lineLimit={lineLimit}>
            <MarkdownViewContainer scrollableY={scrollableY} ref={measuredRef}>
                <StyledEditor
                    customStyle={customStyle}
                    limit={isShowingMore ? null : lineLimit}
                    content={content}
                    readOnly
                />
            </MarkdownViewContainer>
            {hideShowMore && <>...</>}

            {!hideShowMore &&
                (isShowingMore || isTruncated) && ( // "show more" when isTruncated, "show less" when isShowingMore
                    <ShowMoreWrapper>
                        <CustomButton
                            type="link"
                            onClick={() => (handleShowMore ? handleShowMore() : setIsShowingMore(!isShowingMore))}
                        >
                            {isShowingMore ? 'show less' : 'show more'}
                        </CustomButton>
                    </ShowMoreWrapper>
                )}
        </MarkdownContainer>
    );
}
