import React from 'react';

import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';
import { GlobalDefaultViewIcon } from '@app/entityV2/view/shared/GlobalDefaultViewIcon';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the Global Default Item
 */
export const SetGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            dataTestId="view-dropdown-set-global-default"
            tip="Make this View your organization's default. All new users will have this View applied automatically."
            title="Make organization default"
            icon={<GlobalDefaultViewIcon />}
        />
    );
};
