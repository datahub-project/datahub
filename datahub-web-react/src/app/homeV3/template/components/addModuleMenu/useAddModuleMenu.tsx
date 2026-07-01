import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { LinkSimple } from '@phosphor-icons/react/dist/csr/LinkSimple';
import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import { TextT } from '@phosphor-icons/react/dist/csr/TextT';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import { MenuProps } from 'antd';
import i18next from 'i18next';
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
        get name() {
            return i18next.t('modules:yourAssets.moduleName');
        },
        type: DataHubPageModuleType.OwnedAssets,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

const DOMAINS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:top_domains',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:domains.moduleName');
        },
        type: DataHubPageModuleType.Domains,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

const PLATFORMS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:platforms',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:platforms.moduleName');
        },
        type: DataHubPageModuleType.Platforms,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const ASSETS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:assets',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:assets.moduleName');
        },
        type: DataHubPageModuleType.Assets,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const CHILD_HIERARCHY_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:child_hierarchy',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:childHierarchy.moduleName');
        },
        type: DataHubPageModuleType.ChildHierarchy,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const DATA_PRODUCTS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:data_products',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:dataProducts.moduleName');
        },
        type: DataHubPageModuleType.DataProducts,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const RELATED_TERMS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:related_terms',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:relatedTerms.moduleName');
        },
        type: DataHubPageModuleType.RelatedTerms,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export const LINEAGE_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:lineage',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:lineage.moduleName');
        },
        type: DataHubPageModuleType.Lineage,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};
export const COLUMNS_MODULE: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:columns',
    type: EntityType.DatahubPageModule,
    properties: {
        get name() {
            return i18next.t('modules:columns.moduleName');
        },
        type: DataHubPageModuleType.Columns,
        visibility: { scope: PageModuleScope.Global },
        params: {},
    },
};

export default function useAddModuleMenu(position: ModulePositionInput, closeMenu: () => void) {
    const { t } = useTranslation('modules');
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
            name: t('link.moduleName'),
            key: 'quick-link',
            label: (
                <MenuItem
                    description={t('link.moduleDescription')}
                    title={t('link.moduleName')}
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
            name: t('documentation.moduleName'),
            key: 'documentation',
            label: (
                <MenuItem
                    description={t('documentation.moduleDescription')}
                    title={t('documentation.moduleName')}
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
            name: t('assetCollection.moduleName'),
            key: 'asset-collection',
            label: (
                <MenuItem
                    description={t('assetCollection.moduleDescription')}
                    title={t('assetCollection.moduleName')}
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
            title: t('hierarchy.moduleName'),
            key: 'hierarchyView',
            label: (
                <MenuItem
                    description={t('hierarchy.moduleDescription')}
                    title={t('hierarchy.moduleName')}
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
            name: t('yourAssets.moduleName'),
            key: 'your-assets',
            label: (
                <MenuItem
                    description={t('yourAssets.moduleDescription')}
                    title={t('yourAssets.moduleName')}
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
            name: t('domains.moduleName'),
            key: 'domains',
            label: (
                <MenuItem
                    description={t('domains.moduleDescription')}
                    title={t('domains.moduleName')}
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
            name: t('platforms.moduleName'),
            key: 'platforms',
            label: (
                <MenuItem
                    description={t('platforms.moduleDescription')}
                    title={t('platforms.moduleName')}
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
            name: t('assets.moduleName'),
            key: 'assets',
            label: (
                <MenuItem
                    description={t('assets.moduleDescription')}
                    title={t('assets.moduleName')}
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
            name: t('hierarchy.moduleName'),
            key: 'hierarchy',
            label: (
                <MenuItem
                    description={t('childHierarchy.moduleDescription')}
                    title={
                        entityType === EntityType.Domain
                            ? t('childHierarchy.menu.domainsTitle')
                            : t('childHierarchy.menu.contentsTitle')
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
            name: t('dataProducts.moduleName'),
            key: 'dataProducts',
            label: (
                <MenuItem
                    description={t('dataProducts.moduleDescription')}
                    title={t('dataProducts.moduleName')}
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
            name: t('relatedTerms.moduleName'),
            key: 'relatedTerms',
            label: (
                <MenuItem
                    description={t('relatedTerms.moduleDescription')}
                    title={t('relatedTerms.moduleName')}
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
            name: t('lineage.moduleName'),
            key: 'lineage',
            label: (
                <MenuItem
                    description={t('lineage.moduleDescription')}
                    title={t('lineage.moduleName')}
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
            name: t('columns.moduleName'),
            key: 'columns',
            label: (
                <MenuItem
                    description={t('columns.moduleDescription')}
                    title={t('columns.moduleName')}
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
