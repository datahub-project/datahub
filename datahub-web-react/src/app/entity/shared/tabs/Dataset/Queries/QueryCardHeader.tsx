/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ExpandOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import CopyQuery from '@app/entity/shared/tabs/Dataset/Queries/CopyQuery';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: right;
`;

const Actions = styled.div<{ opacity?: number }>`
    padding: 0px;
    height: 0px;
    transform: translate(-12px, 12px);
    opacity: ${(props) => props.opacity || 1.0};
`;

const ExpandButton = styled(Button)`
    margin-left: 8px;
`;

export type Props = {
    query: string;
    focused: boolean;
    onClickExpand?: (newQuery) => void;
};

export default function QueryCardHeader({ query, focused, onClickExpand }: Props) {
    return (
        <Header>
            <Actions opacity={(!focused && 0.3) || 1.0}>
                <CopyQuery query={query} />
                <ExpandButton onClick={onClickExpand}>
                    <ExpandOutlined />
                </ExpandButton>
            </Actions>
        </Header>
    );
}
