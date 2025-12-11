/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { StyledMenuItem } from '@app/shared/share/v2/styledComponents';

interface CopyNameMenuItemProps {
    key: string;
    type: string;
    name: string;
    qualifiedName?: string;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
`;

export default function CopyNameMenuItem({ key, type, name, qualifiedName }: CopyNameMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                if (qualifiedName) {
                    navigator.clipboard.writeText(qualifiedName);
                } else {
                    navigator.clipboard.writeText(name);
                }
                setIsClicked(true);
            }}
        >
            <Tooltip title={`Copy the full name of the ${type}`}>
                {isClicked ? <CheckOutlined /> : <CopyOutlined />}
                <TextSpan>
                    <b>Copy Name</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
