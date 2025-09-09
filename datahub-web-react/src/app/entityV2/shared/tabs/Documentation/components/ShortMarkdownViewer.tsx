import { Text } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';

const StyledText = styled(Text)`
    text-wrap: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    width: 100%;
`;

export type Props = {
    content: string;
    clearMarkdown?: boolean;
};

export default function ShortMarkdownViewer({ content, clearMarkdown }: Props) {
    const processedContent = useMemo(() => {
        let processingContent = content;

        if (clearMarkdown) {
            processingContent = removeMarkdown(processingContent);
        }

        return processingContent;
    }, [content, clearMarkdown]);

    return (
        <StyledText color="gray" colorLevel={1700} type="div">
            {processedContent}
        </StyledText>
    );
}
