import { StopOutlined } from '@ant-design/icons';
import React from 'react';

import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the User's default view item
 */
export const RemoveUserDefaultItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            dataTestId="view-dropdown-remove-user-default"
            tip="Remove this View as your personal default."
            title="Remove as default"
            icon={<StopOutlined />}
        />
    );
};
