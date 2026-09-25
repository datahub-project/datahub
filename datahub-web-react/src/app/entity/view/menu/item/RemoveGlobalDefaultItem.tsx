import { StopOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

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
                icon={<StopOutlined />}
            />
        </MenuItemStyle>
    );
};
