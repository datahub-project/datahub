import React from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { EmptyTab } from '../../../components/styled/EmptyTab';

export type Props = {
    readOnly?: boolean;
    onClickAddQuery: () => void;
};

export default function EmptyQueries({ readOnly = false, onClickAddQuery }: Props) {
    return (
        <EmptyTab tab="queries">
            {!readOnly && (
                <Button onClick={onClickAddQuery}>
                    <PlusOutlined /> Add Query
                </Button>
            )}
        </EmptyTab>
    );
}
