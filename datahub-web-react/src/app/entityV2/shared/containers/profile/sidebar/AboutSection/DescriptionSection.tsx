import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { MarkdownView } from '../../../../components/legacy/MarkdownViewer';
import NoMarkdownViewer, { removeMarkdown } from '../../../../components/styled/StripMarkdownText';
import { REDESIGN_COLORS } from '../../../../constants';

const ABBREVIATED_LIMIT = 150;

const ContentWrapper = styled.div`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.DARK_GREY};
    line-height: 20px;
    ${MarkdownView} {
        font-size: 12px;
    }
`;
const ReadLessContainer = styled.div`
    margin-top: 5px;
`;
const ReadLessText = styled(Typography.Link)``;

interface Props {
    description: string;
    isExpandable?: boolean;
    limit?: number;
}

export default function DescriptionSection({ description, isExpandable, limit }: Props) {
    const isOverLimit = description && removeMarkdown(description).length > ABBREVIATED_LIMIT;
    const shouldShowReadMore = isExpandable && isOverLimit;
    const [expanded, setExpanded] = useState<boolean>(false);

    function readMore() {
        setExpanded(true);
    }

    return (
        <ContentWrapper>
            {!expanded ? (
                <NoMarkdownViewer
                    limit={limit || ABBREVIATED_LIMIT}
                    readMore={
                        shouldShowReadMore ? (
                            <Typography.Link onClick={readMore}> Read More</Typography.Link>
                        ) : undefined
                    }
                    shouldWrap
                >
                    {description}
                </NoMarkdownViewer>
            ) : (
                <>
                    <NoMarkdownViewer shouldWrap>{description}</NoMarkdownViewer>
                    {isOverLimit && (
                        <ReadLessContainer>
                            <ReadLessText
                                onClick={() => {
                                    setExpanded(false);
                                }}
                            >
                                Read Less
                            </ReadLessText>
                        </ReadLessContainer>
                    )}
                </>
            )}
        </ContentWrapper>
    );
}
