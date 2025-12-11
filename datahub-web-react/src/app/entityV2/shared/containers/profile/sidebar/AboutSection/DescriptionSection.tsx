/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
