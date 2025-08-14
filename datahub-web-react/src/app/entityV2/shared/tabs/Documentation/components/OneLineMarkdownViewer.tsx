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

// TODO:: rename it
export default function OneLineMarkdownViewer({ content, clearMarkdown }: Props) {
    const processedContent = useMemo(() => {
        let processingContent = content;
        // get only the first line of content
        // eslint-disable-next-line prefer-destructuring
        // processingContent = processingContent.split('\n')[0];

        if (clearMarkdown) {
            processingContent = removeMarkdown(processingContent);
        }

        return processingContent;
    }, [content, clearMarkdown]);

    return (
            <StyledText color="gray" colorLevel={1700} type='div'>{processedContent}</StyledText>
    );
}
