import { MenuProps } from 'antd';
import { ItemType } from 'antd/lib/menu/hooks/useItems';
import React, { useCallback, useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { ModuleInfo, ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import { convertModuleToModuleInfo } from '@app/homeV3/modules/utils';
import GroupItem from '@app/homeV3/template/components/addModuleMenu/components/GroupItem';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import ModuleMenuItem from '@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem';
import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

export default function useAddModuleMenu(
    modulesAvailableToAdd: ModulesAvailableToAdd,
    position: AddModuleHandlerInput,
    closeMenu: () => void,
): MenuProps {
    const { addModule, createModule } = usePageTemplateContext();

    const handleAddExistingModule = useCallback(
        (module: PageModuleFragment) => {
            addModule({
                module: module as PageModuleFragment,
                position,
            });
            closeMenu();
        },
        [addModule, position, closeMenu],
    );

    const handleCreateNewModule = useCallback(
        (type: DataHubPageModuleType, name: string) => {
            createModule({
                name,
                type,
                position,
            });
            closeMenu();
        },
        [createModule, position, closeMenu],
    );

    return useMemo(() => {
        const items: MenuProps['items'] = [];

        const quickLink = {
            title: 'Quick Link',
            key: 'quick-link',
            label: <MenuItem description="Choose links that are important" title="Quick Link" icon="LinkSimple" />,
            onClick: () => {
                // TODO: open up modal to add a quick link
            },
        };

        const documentation = {
            title: 'Documentation',
            key: 'documentation',
            label: <MenuItem description="Pin docs for your DataHub users" title="Documentation" icon="TextT" />,
            onClick: () => {
                // TODO: open up modal to add documentation
            },
        };

        items.push({
            key: 'customModulesGroup',
            label: <GroupItem title="Custom" />,
            type: 'group',
            children: [quickLink, documentation],
        });

        const yourAssets = {
            title: 'Your Assets',
            key: 'your-assets',
            label: <MenuItem description="Assets the current user owns" title="Your Assets" icon="Database" />,
            onClick: () => {
                handleAddExistingModule(YOUR_ASSETS_MODULE);
            },
        };

        const domains = {
            title: 'Domains',
            key: 'domains',
            label: <MenuItem description="Most used domains in your organization" title="Domains" icon="Globe" />,
            onClick: () => {
                handleAddExistingModule(DOMAINS_MODULE);
            },
        };

        items.push({
            key: 'customLargeModulesGroup',
            label: <GroupItem title="Custom Large" />,
            type: 'group',
            children: [yourAssets, domains],
        });

        // Add admin created modules if available
        if (modulesAvailableToAdd.adminCreatedModules.length) {
            const adminModuleItems = modulesAvailableToAdd.adminCreatedModules.map((module) => ({
                title: module.properties.name,
                key: module.urn,
                label: <ModuleMenuItem module={convertModuleToModuleInfo(module)} />,
                onClick: () => handleAddExistingModule(module),
            }));

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
                children: adminModuleItems,
            });
        }

        return { items };
    }, [modulesAvailableToAdd, handleAddExistingModule, handleCreateNewModule]);
}

const YOUR_ASSETS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:your_assets',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Your Assets',
        type: DataHubPageModuleType.OwnedAssets,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

const DOMAINS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:top_domains',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Domains',
        type: DataHubPageModuleType.Domains,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};
