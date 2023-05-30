import { Button, Typography } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { useRefetch, useRouteToTab } from '../../../../EntityContext';
import { EMPTY_MESSAGES } from '../../../../constants';
import { AddLinkModal } from '../../../../components/styled/AddLinkModal';

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
