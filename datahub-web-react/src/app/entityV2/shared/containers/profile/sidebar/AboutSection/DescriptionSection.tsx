import React from 'react';
import styled from 'styled-components/macro';
import MarkdownViewer from '../../../../components/legacy/MarkdownViewer';
import { REDESIGN_COLORS } from '../../../../constants';

const ABBREVIATED_LIMIT = 150;

const ContentWrapper = styled.div`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.DARK_GREY};
    line-height: 20px;
    white-space: break-spaces;
`;

interface Props {
    description: string;
    isExpandable?: boolean;
    limit?: number;
}

export default function DescriptionSection({ description, isExpandable, limit }: Props) {
    return (
        <ContentWrapper>
            <MarkdownViewer
                limit={limit || ABBREVIATED_LIMIT}
                source={description}
                editable={false}
                ignoreLimit={!isExpandable}
                isCompact
            />
        </ContentWrapper>
    );
}
