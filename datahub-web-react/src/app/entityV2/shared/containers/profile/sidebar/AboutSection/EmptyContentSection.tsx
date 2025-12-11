/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useRouteToTab } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';

const EmptyContentWrapper = styled.div`
    font-size: 12px;
    display: flex;
    justify-content: start;
    align-items: start;
`;

const EmptyContentMessage = styled(Typography.Text)`
    font-size: 12px;
`;

const AddButton = styled.div`
    margin: 0px;
    padding: 0px;
    margin-left: 12px;
    :hover {
        cursor: pointer;
    }
`;

interface Props {
    readOnly?: boolean;
}

export default function EmptyContentSection({ readOnly }: Props) {
    const routeToTab = useRouteToTab();

    return (
        <EmptyContentWrapper>
            <EmptyContentMessage type="secondary">{EMPTY_MESSAGES.documentation.title}</EmptyContentMessage>
            {!readOnly && (
                <AddButton onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}>
                    <PlusOutlined style={{ fontSize: 10, marginRight: 6 }} /> Add docs
                </AddButton>
            )}
        </EmptyContentWrapper>
    );
}
