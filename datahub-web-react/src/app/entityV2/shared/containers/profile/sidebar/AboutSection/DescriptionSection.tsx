import React from 'react';
import styled from 'styled-components/macro';
import CompactMarkdownViewer from '../../../../tabs/Documentation/components/CompactMarkdownViewer';
import { REDESIGN_COLORS } from '../../../../constants';

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
    lineLimit?: number;
    isShowMoreEnabled?: boolean;
}

export default function DescriptionSection({ description, isExpandable, lineLimit, isShowMoreEnabled }: Props) {
    return (
        <ContentWrapper>
            <CompactMarkdownViewer
                lineLimit={isExpandable ? lineLimit : null}
                content={description}
                isShowMoreEnabled={isShowMoreEnabled}
            />
        </ContentWrapper>
    );
}
