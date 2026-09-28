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
 * Remove the User's default view item
 */
export const RemoveUserDefaultItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-remove-user-default">
            <IconItemTitle
                tip={t('menu.removeDefaultTooltip')}
                title={t('menu.removeDefault')}
                icon={<StopOutlined />}
            />
        </MenuItemStyle>
    );
};
