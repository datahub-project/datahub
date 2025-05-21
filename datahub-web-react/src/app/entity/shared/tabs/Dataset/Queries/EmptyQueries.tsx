import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import { EmptyTab } from '@app/entity/shared/components/styled/EmptyTab';

export type Props = {
    message?: string;
    readOnly?: boolean;
    onClickAddQuery: () => void;
};

export default function EmptyQueries({ message, readOnly = false, onClickAddQuery }: Props) {
    return (
        <EmptyTab tab="queries">
            {!readOnly && !message && (
                <Button onClick={onClickAddQuery}>
                    <PlusOutlined /> Add Query
                </Button>
            )}
        </EmptyTab>
    );
}
