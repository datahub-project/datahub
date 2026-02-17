import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EmptyTab } from '@app/entity/shared/components/styled/EmptyTab';

const StyledEmpty = styled(Empty)`
    padding: 40px;
`;

export type Props = {
    message?: string;
    emptyText?: string;
    readOnly?: boolean;
    hideButton?: boolean;
    onClickAddQuery?: () => void;
};

export default function EmptyQueries({
    message,
    emptyText,
    readOnly = false,
    hideButton = false,
    onClickAddQuery,
}: Props) {
    if (emptyText) {
        return <StyledEmpty description={<Typography.Text type="secondary">{emptyText}</Typography.Text>} />;
    }

    return (
        <EmptyTab tab="queries">
            {!readOnly && !message && !hideButton && onClickAddQuery && (
                <Button onClick={onClickAddQuery}>
                    <PlusOutlined /> Add Query
                </Button>
            )}
        </EmptyTab>
    );
}
