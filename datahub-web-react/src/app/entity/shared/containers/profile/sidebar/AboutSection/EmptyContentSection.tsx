/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EditOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entity/shared/components/styled/AddLinkModal';
import { EMPTY_MESSAGES } from '@app/entity/shared/constants';

const EmptyContentWrapper = styled.div`
    margin-bottom: 5px;
    font-size: 14px;
`;

const EmptyContentMessage = styled(Typography.Paragraph)`
    font-size: 12px;
`;

const AddLinkWrapper = styled.span`
    margin-left: 8px;
`;

interface Props {
    hideLinksButton?: boolean;
    readOnly?: boolean;
}

export default function EmptyContentSection({ hideLinksButton, readOnly }: Props) {
    const routeToTab = useRouteToTab();
    const refetch = useRefetch();

    return (
        <EmptyContentWrapper>
            <>
                <EmptyContentMessage type="secondary">
                    {EMPTY_MESSAGES.documentation.title}. {EMPTY_MESSAGES.documentation.description}
                </EmptyContentMessage>
                {!readOnly && (
                    <Button onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}>
                        <EditOutlined /> Add Documentation
                    </Button>
                )}
                {!readOnly && !hideLinksButton && (
                    <AddLinkWrapper>
                        <AddLinkModal refetch={refetch} />
                    </AddLinkWrapper>
                )}
            </>
        </EmptyContentWrapper>
    );
}
