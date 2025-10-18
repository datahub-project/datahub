import { MenuProps } from 'antd';
import React, { useCallback, useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { convertModuleToModuleInfo } from '@app/homeV3/modules/utils';
import GroupItem from '@app/homeV3/template/components/addModuleMenu/components/GroupItem';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import ModuleMenuItem from '@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem';
import { getCustomGlobalModules } from '@app/homeV3/template/components/addModuleMenu/utils';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateSurfaceType } from '@types';

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

const PLATFORMS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:platforms',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Platforms',
        type: DataHubPageModuleType.Platforms,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const ASSETS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:assets',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Assets',
        type: DataHubPageModuleType.Assets,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const CHILD_HIERARCHY_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:child_hierarchy',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Children',
        type: DataHubPageModuleType.ChildHierarchy,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const DATA_PRODUCTS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:data_products',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Data Products',
        type: DataHubPageModuleType.DataProducts,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const RELATED_TERMS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:related_terms',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Related Terms',
        type: DataHubPageModuleType.RelatedTerms,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export default function useAddModuleMenu(position: ModulePositionInput, closeMenu: () => void) {
    const { entityType } = useEntityData();
    const {
        addModule,
        moduleModalState: { open: openModal },
        globalTemplate,
        templateType,
    } = usePageTemplateContext();

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
                    isSmallModule
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Link);
            },
            'data-testid': 'add-link-module',
        };

        const documentation = {
            name: 'Documentation',
            key: 'documentation',
            label: (
                <MenuItem
                    description="Pin docs for your DataHub users"
                    title="Documentation"
                    icon="TextT"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.RichText);
            },
            'data-testid': 'add-documentation-module',
        };

        const assetCollection = {
            name: 'Collection',
            key: 'asset-collection',
            label: (
                <MenuItem
                    description="A curated list of assets of your choosing"
                    title="Collection"
                    icon="Stack"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.AssetCollection);
            },
            'data-testid': 'add-asset-collection-module',
        };

        const hierarchyView = {
            title: 'Hierarchy',
            key: 'hierarchyView',
            label: <MenuItem description="Top down view of assets" title="Hierarchy" icon="Globe" />,
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Hierarchy);
            },
            'data-testid': 'add-hierarchy-module',
        };

        const customHomeModules = [quickLink, assetCollection, documentation, hierarchyView];
        const customSummaryModules = [assetCollection, documentation, hierarchyView];

        const finalCustomModules =
            templateType === PageTemplateSurfaceType.HomePage ? customHomeModules : customSummaryModules;

        items.push({
            key: 'customModulesGroup',
            label: <GroupItem title="Create Your Own" />,
            type: 'group',
            children: finalCustomModules,
        });

        const yourAssets = {
            name: 'Your Assets',
            key: 'your-assets',
            label: (
                <MenuItem
                    description="Assets the current user owns"
                    title="Your Assets"
                    icon="Database"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(YOUR_ASSETS_MODULE);
            },
            'data-testid': 'add-your-assets-module',
        };

        const domains = {
            name: 'Domains',
            key: 'domains',
            label: (
                <MenuItem
                    description="Most used domains in your organization"
                    title="Domains"
                    icon="Globe"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(DOMAINS_MODULE);
            },
            'data-testid': 'add-domains-module',
        };

        const platforms = {
            name: 'Platforms',
            key: 'platforms',
            label: (
                <MenuItem
                    description="Most used platforms in your organization"
                    title="Platforms"
                    icon="Database"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(PLATFORMS_MODULE);
            },
            'data-testid': 'add-platforms-module',
        };

        const assets = {
            name: 'Assets',
            key: 'assets',
            label: (
                <MenuItem
                    description="Related Assets tagged with the parent entity"
                    title="Assets"
                    icon="Database"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(ASSETS_MODULE);
            },
            'data-testid': 'add-assets-module',
        };

        const childHierarchy = {
            name: 'Hierarchy',
            key: 'hierarchy',
            label: (
                <MenuItem
                    description="View the hierarchy of this asset's children"
                    title={entityType === EntityType.Domain ? 'Domains' : 'Contents'}
                    icon="Globe"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(CHILD_HIERARCHY_MODULE);
            },
            'data-testid': 'add-child-hierarchy-module',
        };

        const dataProducts = {
            name: 'DataProducts',
            key: 'dataProducts',
            label: (
                <MenuItem
                    description="View the data products inside of this domain"
                    title="Data Products"
                    icon="FileText"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(DATA_PRODUCTS_MODULE);
            },
            'data-testid': 'add-data-products-module',
        };

        const relatedTerms = {
            name: 'RelatedTerms',
            key: 'relatedTerms',
            label: (
                <MenuItem
                    description="View the related terms inside of this glossary term"
                    title="Related Terms"
                    icon="FileText"
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(RELATED_TERMS_MODULE);
            },
            'data-testid': 'add-related-terms-module',
        };

        const defaultHomeModules = [yourAssets, domains, platforms];
        // TODO: make this a function to pull out and write unit tests for
        let defaultSummaryModules = [assets];
        if (entityType === EntityType.Domain) {
            defaultSummaryModules = [...defaultSummaryModules, childHierarchy, dataProducts];
        } else if (entityType === EntityType.GlossaryNode) {
            defaultSummaryModules = [childHierarchy];
        } else if (entityType === EntityType.GlossaryTerm) {
            defaultSummaryModules = [...defaultSummaryModules, relatedTerms];
        }

        const finalDefaultModules =
            templateType === PageTemplateSurfaceType.HomePage ? defaultHomeModules : defaultSummaryModules;

        items.push({
            key: 'customLargeModulesGroup',
            label: <GroupItem title="Default" />,
            type: 'group',
            children: finalDefaultModules,
        });

        // Add global custom modules if available
        const customGlobalModules: PageModuleFragment[] = getCustomGlobalModules(globalTemplate);
        if (customGlobalModules.length > 0) {
            const adminModuleItems = customGlobalModules.map((module) => ({
                name: module.properties.name,
                key: module.urn,
                label: (
                    <ModuleMenuItem
                        module={convertModuleToModuleInfo(module)}
                        isSmallModule={SMALL_MODULE_TYPES.includes(module.properties.type)}
                    />
                ),
                onClick: () => handleAddExistingModule(module),
                'data-testid': 'home-default-submenu-option',
            }));

            const homeDefaults = {
                key: 'adminCreatedModulesGroup',
                name: 'Home Defaults',
                label: (
                    <MenuItem
                        icon="Database"
                        title="Home Defaults"
                        description="Modules created for your organization"
                        hasChildren
                    />
                ),
                expandIcon: <></>, // hide the default expand icon
                popupClassName: RESET_DROPDOWN_MENU_STYLES_CLASSNAME, // reset styles of submenu
                children: adminModuleItems,
                'data-testid': 'home-default-modules',
            };

            if (templateType === PageTemplateSurfaceType.HomePage) {
                items.push({
                    key: 'sharedModulesGroup',
                    label: <GroupItem title="Shared" />,
                    type: 'group',
                    children: [homeDefaults],
                });
            }
        }

        return { items };
    }, [globalTemplate, handleOpenCreateModuleModal, handleAddExistingModule, entityType, templateType]);

    return menu;
}
