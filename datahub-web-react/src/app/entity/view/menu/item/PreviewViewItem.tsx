import { EyeOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Preview View Menu Item
 */
export const PreviewViewItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
    return (
        <MenuItemStyle key={key} onClick={onClick}>
            <IconItemTitle tip={t('menu.previewTooltip')} title={tc('preview')} icon={<EyeOutlined />} />
        </MenuItemStyle>
    );
};
