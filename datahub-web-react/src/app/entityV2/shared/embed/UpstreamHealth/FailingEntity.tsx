/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { red } from '@ant-design/colors';
import Icon from '@ant-design/icons/lib/components/Icon';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import ExternalLink from '@images/link-out.svg?react';

const DatasetWrapper = styled.div`
    display: flex;
    align-items: center;
    margin-top: 8px;
    overflow: hidden;
    justify-content: space-between;
`;

const AssertionsSummaryWrapper = styled.span`
    font-size: 10px;
    font-weight: 700;
    line-height: 13px;
    color: ${red[7]};
    background-color: ${red[0]};
    border-radius: 8px;
    margin-left: 5px;
    padding: 2px 4px;
    letter-spacing: 0.2px;
    white-space: nowrap;
`;

const StyledLink = styled(Typography.Link)`
    align-items: center;
    display: flex;
    overflow: hidden;
    img {
        margin-right: 3px;
    }
`;

const StyledIcon = styled(Icon)`
    margin-right: 3px;
`;

interface Props {
    link: string;
    displayName: string;
    contentText: string;
}

export default function FailingEntity({ link, displayName, contentText }: Props) {
    return (
        <DatasetWrapper>
            <StyledLink href={link} target="_blank" rel="noopener noreferrer">
                <StyledIcon component={ExternalLink} />
                <Typography.Text ellipsis={{ tooltip: true }} style={{ color: 'inherit' }}>
                    {displayName}
                </Typography.Text>
            </StyledLink>
            <AssertionsSummaryWrapper>{contentText}</AssertionsSummaryWrapper>
        </DatasetWrapper>
    );
}
