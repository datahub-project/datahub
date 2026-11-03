import { PencilLine } from '@phosphor-icons/react/dist/csr/PencilLine';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { MenuItemStyle } from '@app/entityV2/view/menu/item/styledComponent';
import { IconItemTitle } from '@app/search/view/menu/item/IconItemTitle';

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
            <IconItemTitle tip={t('menu.editTooltip')} title={tc('edit')} icon={<PencilLine />} />
        </MenuItemStyle>
    );
};
