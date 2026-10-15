import { Menu } from '@components';
import { ShareNetwork } from '@phosphor-icons/react/dist/csr/ShareNetwork';
import React from 'react';

import {
    ActionMenuItem,
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { useShareMenuItems } from '@app/shared/share/v2/useShareMenuItems';

export default function ShareMenuAction() {
    const items = useShareMenuItems();

    return (
        <Menu items={items} trigger={['hover']} placement="bottomRight">
            <ActionMenuItem key="share" data-testid="share-menu-action">
                <ShareNetwork size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
            </ActionMenuItem>
        </Menu>
    );
}
