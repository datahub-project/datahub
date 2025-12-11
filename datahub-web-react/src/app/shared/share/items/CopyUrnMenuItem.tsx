/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

interface CopyUrnMenuItemProps {
    urn: string;
    type: string;
}

const StyledMenuItem = styled.div`
    && {
        color: ${ANTD_GRAY[8]};
    }
`;

const TextSpan = styled.span`
    padding-left: 12px;
`;

export default function CopyUrnMenuItem({ urn, type }: CopyUrnMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);

    return (
        <StyledMenuItem
            onClick={() => {
                navigator.clipboard.writeText(urn);
                setIsClicked(true);
            }}
        >
            <Tooltip title={`Copy the URN for this ${type}. An URN uniquely identifies an entity on DataHub.`}>
                {isClicked ? <CheckOutlined /> : <CopyOutlined />}
                <TextSpan>
                    <b>Copy URN</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
