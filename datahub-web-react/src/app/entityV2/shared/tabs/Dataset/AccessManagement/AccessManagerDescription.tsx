/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

export type Props = {
    description: any;
};

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 500px;
    height: 100%;
    min-height: 22px;
`;

export default function AccessManagerDescription({ description }: Props) {
    const shouldTruncateDescription = description.length > 150;
    const [expanded, setIsExpanded] = useState(!shouldTruncateDescription);
    const finalDescription = expanded ? description : description.slice(0, 150);
    const toggleExpanded = () => {
        setIsExpanded(!expanded);
    };

    return (
        <DescriptionContainer>
            {finalDescription}
            <Typography.Link
                onClick={() => {
                    toggleExpanded();
                }}
            >
                {(shouldTruncateDescription && (expanded ? ' Read Less' : '...Read More')) || undefined}
            </Typography.Link>
        </DescriptionContainer>
    );
}
