import { FormOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Edit View Menu Item
 */
export const EditViewItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-edit">
            <IconItemTitle tip={t('menu.editTooltip')} title={tc('edit')} icon={<FormOutlined />} />
        </MenuItemStyle>
    );
};
