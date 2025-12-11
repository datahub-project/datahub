/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const StyledMenuItem = styled.div`
    && {
        color: ${ANTD_GRAY[8]};
    }
`;

const TextSpan = styled.span`
    padding-left: 12px;
`;

const StyledLinkOutlined = styled(LinkOutlined)`
    font-size: 14px;
`;

export default function CopyLinkMenuItem() {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);

    return (
        <StyledMenuItem
            onClick={() => {
                navigator.clipboard.writeText(window.location.href);
                setIsClicked(true);
            }}
        >
            <Tooltip title="Copy a shareable link to this entity.">
                {isClicked ? <CheckOutlined /> : <StyledLinkOutlined />}
                <TextSpan>
                    <b>Copy Link</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
