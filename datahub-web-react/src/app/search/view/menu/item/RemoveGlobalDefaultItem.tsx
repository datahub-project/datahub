import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { MenuItemStyle } from '@app/entityV2/view/menu/item/styledComponent';
import { IconItemTitle } from '@app/search/view/menu/item/IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    return (
        <MenuItemStyle key={key} onClick={onClick}>
            <IconItemTitle
                tip={t('menu.removeOrgDefaultTooltip')}
                title={t('menu.removeOrgDefault')}
                icon={<Prohibit />}
            />
        </MenuItemStyle>
    );
};
