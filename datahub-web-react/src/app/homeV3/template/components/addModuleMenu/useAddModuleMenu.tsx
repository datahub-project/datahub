import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { LinkSimple } from '@phosphor-icons/react/dist/csr/LinkSimple';
import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import { TextT } from '@phosphor-icons/react/dist/csr/TextT';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import { MenuProps } from 'antd';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

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

export const LINEAGE_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:lineage',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Lineage',
        type: DataHubPageModuleType.Lineage,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};
export const COLUMNS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:columns',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Columns',
        type: DataHubPageModuleType.Columns,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export default function useAddModuleMenu(position: ModulePositionInput, closeMenu: () => void) {
    const { t } = useTranslation([
        'modules',
        'module.assetCollection',
        'module.assets',
        'module.childHierarchy',
        'module.columns',
        'module.dataProducts',
        'module.documentation',
        'module.domains',
        'module.hierarchy',
        'module.lineage',
        'module.link',
        'module.platforms',
        'module.relatedTerms',
        'module.yourAssets',
    ]);
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
            name: t('module.link:menu.title'),
            key: 'quick-link',
            label: (
                <MenuItem
                    description={t('module.link:menu.description')}
                    title={t('module.link:menu.title')}
                    icon={LinkSimple}
                    isSmallModule
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.Link);
            },
            'data-testid': 'add-link-module',
        };

        const documentation = {
            name: t('module.documentation:menu.title'),
            key: 'documentation',
            label: (
                <MenuItem
                    description={t('module.documentation:menu.description')}
                    title={t('module.documentation:menu.title')}
                    icon={TextT}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.RichText);
            },
            'data-testid': 'add-documentation-module',
        };

        const assetCollection = {
            name: t('module.assetCollection:menu.title'),
            key: 'asset-collection',
            label: (
                <MenuItem
                    description={t('module.assetCollection:menu.description')}
                    title={t('module.assetCollection:menu.title')}
                    icon={Stack}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleOpenCreateModuleModal(DataHubPageModuleType.AssetCollection);
            },
            'data-testid': 'add-asset-collection-module',
        };

        const hierarchyView = {
            title: t('module.hierarchy:menu.title'),
            key: 'hierarchyView',
            label: (
                <MenuItem
                    description={t('module.hierarchy:menu.description')}
                    title={t('module.hierarchy:menu.title')}
                    icon={Globe}
                />
            ),
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
            label: <GroupItem title={t('menu.createYourOwn')} />,
            type: 'group',
            children: finalCustomModules,
        });

        const yourAssets = {
            name: t('module.yourAssets:menu.title'),
            key: 'your-assets',
            label: (
                <MenuItem
                    description={t('module.yourAssets:menu.description')}
                    title={t('module.yourAssets:menu.title')}
                    icon={Database}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(YOUR_ASSETS_MODULE);
            },
            'data-testid': 'add-your-assets-module',
        };

        const domains = {
            name: t('module.domains:menu.title'),
            key: 'domains',
            label: (
                <MenuItem
                    description={t('module.domains:menu.description')}
                    title={t('module.domains:menu.title')}
                    icon={Globe}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(DOMAINS_MODULE);
            },
            'data-testid': 'add-domains-module',
        };

        const platforms = {
            name: t('module.platforms:menu.title'),
            key: 'platforms',
            label: (
                <MenuItem
                    description={t('module.platforms:menu.description')}
                    title={t('module.platforms:menu.title')}
                    icon={Database}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(PLATFORMS_MODULE);
            },
            'data-testid': 'add-platforms-module',
        };

        const assets = {
            name: t('module.assets:menu.title'),
            key: 'assets',
            label: (
                <MenuItem
                    description={t('module.assets:menu.description')}
                    title={t('module.assets:menu.title')}
                    icon={Database}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(ASSETS_MODULE);
            },
            'data-testid': 'add-assets-module',
        };

        const childHierarchy = {
            name: t('module.hierarchy:menu.title'),
            key: 'hierarchy',
            label: (
                <MenuItem
                    description={t('module.childHierarchy:menu.description')}
                    title={
                        entityType === EntityType.Domain
                            ? t('module.childHierarchy:menu.domainsTitle')
                            : t('module.childHierarchy:menu.contentsTitle')
                    }
                    icon={Globe}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(CHILD_HIERARCHY_MODULE);
            },
            'data-testid': 'add-child-hierarchy-module',
        };

        const dataProducts = {
            name: t('module.dataProducts:menu.title'),
            key: 'dataProducts',
            label: (
                <MenuItem
                    description={t('module.dataProducts:menu.description')}
                    title={t('module.dataProducts:menu.title')}
                    icon={FileText}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(DATA_PRODUCTS_MODULE);
            },
            'data-testid': 'add-data-products-module',
        };

        const relatedTerms = {
            name: t('module.relatedTerms:menu.title'),
            key: 'relatedTerms',
            label: (
                <MenuItem
                    description={t('module.relatedTerms:menu.description')}
                    title={t('module.relatedTerms:menu.title')}
                    icon={FileText}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(RELATED_TERMS_MODULE);
            },
            'data-testid': 'add-related-terms-module',
        };

        const lineage = {
            name: t('module.lineage:menu.title'),
            key: 'lineage',
            label: (
                <MenuItem
                    description={t('module.lineage:menu.description')}
                    title={t('module.lineage:menu.title')}
                    icon={TreeStructure}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(LINEAGE_MODULE);
            },
            'data-testid': 'add-lineage-module',
        };
        const schemaTable = {
            name: t('module.columns:menu.title'),
            key: 'columns',
            label: (
                <MenuItem
                    description={t('module.columns:menu.description')}
                    title={t('module.columns:menu.title')}
                    icon={Table}
                    isSmallModule={false}
                />
            ),
            onClick: () => {
                handleAddExistingModule(COLUMNS_MODULE);
            },
            'data-testid': 'add-columns-module',
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
        } else if (entityType === EntityType.Dataset) {
            defaultSummaryModules = [schemaTable, lineage];
        }

        const finalDefaultModules =
            templateType === PageTemplateSurfaceType.HomePage ? defaultHomeModules : defaultSummaryModules;

        items.push({
            key: 'customLargeModulesGroup',
            label: <GroupItem title={t('menu.default')} />,
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
                name: t('menu.homeDefaults'),
                label: (
                    <MenuItem
                        icon={Database}
                        title={t('menu.homeDefaults')}
                        description={t('menu.homeDefaultsDescription')}
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
                    label: <GroupItem title={t('menu.shared')} />,
                    type: 'group',
                    children: [homeDefaults],
                });
            }
        }

        return { items };
    }, [t, globalTemplate, handleOpenCreateModuleModal, handleAddExistingModule, entityType, templateType]);

    return menu;
}
