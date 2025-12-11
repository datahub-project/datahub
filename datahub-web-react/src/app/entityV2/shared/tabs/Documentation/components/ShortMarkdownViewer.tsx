/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
