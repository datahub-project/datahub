import { MenuProps } from 'antd';
import React, { useCallback, useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { LARGE_MODULE_TYPES, SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import { convertModuleToModuleInfo } from '@app/homeV3/modules/utils';
import GroupItem from '@app/homeV3/template/components/addModuleMenu/components/GroupItem';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import ModuleMenuItem from '@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem';
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

export default function useAddModuleMenu(
    modulesAvailableToAdd: ModulesAvailableToAdd,
    position: ModulePositionInput,
    closeMenu: () => void,
) {
    const {
        addModule,
        moduleModalState: { open: openModal },
        template,
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
            title: 'Quick Link',
            key: 'quick-link',
            label: <MenuItem description="Choose links that are important" title="Quick Link" icon="LinkSimple" />,
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Link);
            },
            disabled: isLargeModuleRow,
        };

        const documentation = {
            title: 'Documentation',
            key: 'documentation',
            label: <MenuItem description="Pin docs for your DataHub users" title="Documentation" icon="TextT" />,
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.RichText);
            },
            disabled: isSmallModuleRow,
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
            disabled: isSmallModuleRow,
        };

        const domains = {
            title: 'Domains',
            key: 'domains',
            label: <MenuItem description="Most used domains in your organization" title="Domains" icon="Globe" />,
            onClick: () => {
                handleAddExistingModule(DOMAINS_MODULE);
            },
            disabled: isSmallModuleRow,
        };

        const assetCollection = {
            title: 'Asset Collection',
            key: 'asset-collection',
            label: (
                <MenuItem
                    description="A curated list of assets of your choosing"
                    title="Asset Collection"
                    icon="Stack"
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.AssetCollection);
            },
            disabled: isSmallModuleRow,
        };

        const hierarchyView = {
            title: 'Hierarchy View',
            key: 'hierarchyView',
            label: (
                <MenuItem description="Most used domains in your organization" title="Hierarchy View" icon="Globe" />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Hierarchy);
            },
        };

        items.push({
            key: 'customLargeModulesGroup',
            label: <GroupItem title="Custom Large" />,
            type: 'group',
            children: [yourAssets, domains, assetCollection, hierarchyView],
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
    }, [
        isLargeModuleRow,
        isSmallModuleRow,
        modulesAvailableToAdd.adminCreatedModules,
        handleOpenCreateModuleModal,
        handleAddExistingModule,
    ]);

    return menu;
}
