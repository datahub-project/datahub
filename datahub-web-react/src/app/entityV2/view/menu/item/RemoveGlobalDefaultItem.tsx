import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { ViewItem } from './ViewItem';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            tip="Remove this View as your organization's default."
            title="Remove organization default"
            icon={<StopOutlined />}
        />
    );
};
