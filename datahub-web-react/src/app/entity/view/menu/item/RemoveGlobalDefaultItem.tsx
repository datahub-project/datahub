import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <div key={key} onClick={onClick}>
            <IconItemTitle
                tip="Remove this View as your organization's default."
                title="Remove organization default"
                icon={<StopOutlined />}
            />
        </div>
    );
};
