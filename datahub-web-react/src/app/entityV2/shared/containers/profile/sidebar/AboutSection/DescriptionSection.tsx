import React from 'react';
import styled from 'styled-components/macro';

import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';

const ContentWrapper = styled.div`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
    line-height: 20px;
    white-space: break-spaces;
    width: 100%;
`;

interface Props {
    description: string;
    isExpandable?: boolean;
    lineLimit?: number;
}

export default function DescriptionSection({ description, isExpandable, lineLimit }: Props) {
    return (
        <ContentWrapper>
            <CompactMarkdownViewer lineLimit={isExpandable ? lineLimit : null} content={description} />
        </ContentWrapper>
    );
}
