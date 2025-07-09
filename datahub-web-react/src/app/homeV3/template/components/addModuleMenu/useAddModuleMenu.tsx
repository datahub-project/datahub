import { MenuProps } from 'antd';
import { ItemType } from 'antd/lib/menu/hooks/useItems';
import React, { useCallback, useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';

import { ModuleInfo, ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import GroupItem from '@app/homeV3/template/components/addModuleMenu/components/GroupItem';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import ModuleMenuItem from '@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem';

export default function useAddModuleMenu(
    modulesAvailableToAdd: ModulesAvailableToAdd,
    onClick?: (module: ModuleInfo) => void,
): MenuProps {
    const convertModule = useCallback(
        (module: ModuleInfo): ItemType => ({
            title: module.name,
            key: module.key,
            label: <ModuleMenuItem module={module} />,
            onClick: () => onClick?.(module),
        }),
        [onClick],
    );

    return useMemo(() => {
        const items: MenuProps['items'] = [];

        if (modulesAvailableToAdd.customModules.length) {
            items.push({
                key: 'customModulesGroup',
                label: <GroupItem title="Custom" />,
                type: 'group',
                children: modulesAvailableToAdd.customModules.map(convertModule),
            });
        }

        if (modulesAvailableToAdd.customLargeModules.length) {
            items.push({
                key: 'customLargeModulesGroup',
                label: <GroupItem title="Custom Large" />,
                type: 'group',
                children: modulesAvailableToAdd.customLargeModules.map(convertModule),
            });
        }

        if (modulesAvailableToAdd.adminCreatedModules.length) {
            items.push({
                key: 'adminCreatedModulesGroup',
                title: 'Admin Created Widgets',
                label: (
                    <MenuItem
                        icon="Database"
                        title="Admin Created Widgets"
                        description="Your organizations data products"
                        hasChildren
                    />
                ),
                expandIcon: <></>, // hide the default expand icon
                popupClassName: RESET_DROPDOWN_MENU_STYLES_CLASSNAME, // reset styles of submenu
                children: modulesAvailableToAdd.adminCreatedModules.map(convertModule),
            });
        }

        return { items };
    }, [modulesAvailableToAdd, convertModule]);
}
