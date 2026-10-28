import React from 'react';
import { useTranslation } from 'react-i18next';

import { MenuItemStyle } from '@app/entityV2/view/menu/item/styledComponent';
import { IconItemTitle } from '@app/search/view/menu/item/IconItemTitle';
import { GlobalDefaultViewIcon } from '@app/search/view/shared/GlobalDefaultViewIcon';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the Global Default Item
 */
export const SetGlobalDefaultItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-set-global-default">
            <IconItemTitle
                tip={t('menu.makeOrgDefaultTooltipLegacy')}
                title={t('menu.makeOrgDefault')}
                icon={<GlobalDefaultViewIcon />}
            />
        </MenuItemStyle>
    );
};
