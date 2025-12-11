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

const DescriptionText = styled.span`
    color: ${ANTD_GRAY_V2[8]};
`;

const DescriptionSeparator = styled.span`
    margin: 0 8px;
`;

interface Props {
    description: string;
}

export default function ValueDescription({ description }: Props) {
    return (
        <>
            <DescriptionSeparator>-</DescriptionSeparator>
            <DescriptionText>{description}</DescriptionText>
        </>
    );
}
