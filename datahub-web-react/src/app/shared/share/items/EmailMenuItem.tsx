/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import qs from 'query-string';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

interface EmailMenuItemProps {
    urn: string;
    name: string;
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

export default function EmailMenuItem({ urn, name, type }: EmailMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);
    const linkText = window.location.href;

    const link = qs.stringifyUrl({
        url: 'mailto:',
        query: {
            subject: `${name} | ${type}`,
            body: `Check out this ${type} on DataHub: ${linkText}. Urn: ${urn}`,
        },
    });

    return (
        <StyledMenuItem
            onClick={() => {
                setIsClicked(true);
            }}
        >
            <Tooltip title={`Share this ${type} via email`}>
                {isClicked ? <CheckOutlined /> : <MailOutlined />}
                <TextSpan>
                    <a href={link} target="_blank" rel="noreferrer" style={{ color: 'inherit' }}>
                        <b>Email</b>
                    </a>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
