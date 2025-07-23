import { MenuProps } from 'antd';
import React, { useCallback, useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { LARGE_MODULE_TYPES, SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { convertModuleToModuleInfo } from '@app/homeV3/modules/utils';
import GroupItem from '@app/homeV3/template/components/addModuleMenu/components/GroupItem';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import ModuleMenuItem from '@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem';
import { getCustomGlobalModules } from '@app/homeV3/template/components/addModuleMenu/utils';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

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

export default function useAddModuleMenu(position: ModulePositionInput, closeMenu: () => void) {
    const {
        addModule,
        moduleModalState: { open: openModal },
        template,
        globalTemplate,
    } = usePageTemplateContext();

    const isLargeModuleRow =
        position.rowIndex !== undefined &&
        template?.properties.rows[position.rowIndex]?.modules?.some((module) =>
            LARGE_MODULE_TYPES.includes(module.properties.type),
        );
    const isSmallModuleRow =
        position.rowIndex !== undefined &&
        template?.properties.rows[position.rowIndex]?.modules?.some((module) =>
            SMALL_MODULE_TYPES.includes(module.properties.type),
        );

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

    const handleOpenCreateModuleModal = useCallback(
        (type: DataHubPageModuleType) => {
            openModal(type, position);
            closeMenu();
        },
        [openModal, position, closeMenu],
    );

    const menu = useMemo(() => {
        const items: MenuProps['items'] = [];

        const quickLink = {
            name: 'Quick Link',
            key: 'quick-link',
            label: (
                <MenuItem
                    description="Choose links that are important"
                    title="Quick Link"
                    icon="LinkSimple"
                    isDisabled={isLargeModuleRow}
                    isSmallModule
                />
            ),

            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Link);
            },
            disabled: isLargeModuleRow,
        };

        const documentation = {
            name: 'Documentation',
            key: 'documentation',
            label: (
                <MenuItem
                    description="Pin docs for your DataHub users"
                    title="Documentation"
                    icon="TextT"
                    isDisabled={isSmallModuleRow}
                    isSmallModule={false}
                />
            ),

            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.RichText);
            },
            disabled: isSmallModuleRow,
        };

        const assetCollection = {
            name: 'Collection',
            key: 'asset-collection',
            label: (
                <MenuItem
                    description="A curated list of assets of your choosing"
                    title="Collection"
                    icon="Stack"
                    isDisabled={isSmallModuleRow}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.AssetCollection);
            },
            disabled: isSmallModuleRow,
        };

        const hierarchyView = {
            title: 'Hierarchy',
            key: 'hierarchyView',
            label: <MenuItem description="Top down view of assets" title="Hierarchy" icon="Globe" />,
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Hierarchy);
            },
        };

        items.push({
            key: 'customModulesGroup',
            label: <GroupItem title="Create Your Own" />,
            type: 'group',
            children: [quickLink, assetCollection, documentation, hierarchyView],
        });

        const yourAssets = {
            name: 'Your Assets',
            key: 'your-assets',
            label: (
                <MenuItem
                    description="Assets the current user owns"
                    title="Your Assets"
                    icon="Database"
                    isDisabled={isSmallModuleRow}
                    isSmallModule={false}
                />
            ),

            onClick: () => {
                handleAddExistingModule(YOUR_ASSETS_MODULE);
            },
            disabled: isSmallModuleRow,
        };

        const domains = {
            name: 'Domains',
            key: 'domains',
            label: (
                <MenuItem
                    description="Most used domains in your organization"
                    title="Domains"
                    icon="Globe"
                    isDisabled={isSmallModuleRow}
                    isSmallModule={false}
                />
            ),

            onClick: () => {
                handleAddExistingModule(DOMAINS_MODULE);
            },
            disabled: isSmallModuleRow,
        };

        items.push({
            key: 'customLargeModulesGroup',
            label: <GroupItem title="Default by DataHub" />,
            type: 'group',
            children: [yourAssets, domains],
        });

        // Add global custom modules if available
        const customGlobalModules: PageModuleFragment[] = getCustomGlobalModules(globalTemplate);
        if (customGlobalModules.length > 0) {
            const adminModuleItems = customGlobalModules.map((module) => ({
                title: module.properties.name,
                key: module.urn,
                label: <ModuleMenuItem module={convertModuleToModuleInfo(module)} />,
                onClick: () => handleAddExistingModule(module),
            }));

            const homeDefaults = {
                key: 'adminCreatedModulesGroup',
                title: 'Home Defaults',
                label: (
                    <MenuItem
                        icon="Database"
                        title="Home Defaults"
                        description="Your organizations curated widgets"
                        hasChildren
                    />
                ),
                expandIcon: <></>, // hide the default expand icon
                popupClassName: RESET_DROPDOWN_MENU_STYLES_CLASSNAME, // reset styles of submenu
                children: adminModuleItems,
            };

            items.push({
                key: 'sharedModulesGroup',
                label: <GroupItem title="Shared" />,
                type: 'group',
                children: [homeDefaults],
            });
        }

        return { items };
    }, [isLargeModuleRow, isSmallModuleRow, globalTemplate, handleOpenCreateModuleModal, handleAddExistingModule]);

    return menu;
}
