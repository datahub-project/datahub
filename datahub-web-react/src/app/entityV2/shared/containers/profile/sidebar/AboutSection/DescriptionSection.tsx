import React from 'react';
import styled from 'styled-components/macro';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';

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
