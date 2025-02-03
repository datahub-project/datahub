import React from 'react';
import { GlobalDefaultViewIcon } from '../../shared/GlobalDefaultViewIcon';
import { ViewItem } from './ViewItem';

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
