/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { pluralize } from '@app/shared/textUtil';

const OptionalPromptsWrapper = styled.div`
    color: ${ANTD_GRAY_V2[8]};
    margin-top: 4px;
`;

interface Props {
    numRemaining: number;
}

export default function OptionalPromptsRemaining({ numRemaining }: Props) {
    if (numRemaining <= 0) return null;

    return (
        <OptionalPromptsWrapper>
            {numRemaining} additional {pluralize(numRemaining, 'question', 's')} remaining
        </OptionalPromptsWrapper>
    );
}
