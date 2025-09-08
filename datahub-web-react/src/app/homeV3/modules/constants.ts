import { IconNames } from '@components';

import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// TODO: remove these description once descriptions in modules are implemented
export const MODULE_TYPE_TO_DESCRIPTION: Map<DataHubPageModuleType, string> = new Map([
    [DataHubPageModuleType.AssetCollection, 'A curated list of assets of your choosing'],
    [DataHubPageModuleType.Domains, 'Most used domains in your organization'],
    [DataHubPageModuleType.Hierarchy, 'Top down view of assets'],
    [DataHubPageModuleType.Link, 'Choose links that are important'],
    [DataHubPageModuleType.OwnedAssets, 'Assets the current user owns'],
    [DataHubPageModuleType.RichText, 'Pin docs for your DataHub users'],
]);

export const MODULE_TYPE_TO_ICON: Map<DataHubPageModuleType, IconNames> = new Map([
    [DataHubPageModuleType.AssetCollection, 'Stack'],
    [DataHubPageModuleType.Domains, 'Globe'],
    [DataHubPageModuleType.Hierarchy, 'SortAscending'],
    [DataHubPageModuleType.Link, 'LinkSimple'],
    [DataHubPageModuleType.OwnedAssets, 'Database'],
    [DataHubPageModuleType.RichText, 'TextT'],
]);

export const DEFAULT_MODULE_ICON = 'Database';

// keep this in sync with PageModuleService.java
export const DEFAULT_MODULE_URNS = [
    'urn:li:dataHubPageModule:your_assets',
    'urn:li:dataHubPageModule:your_subscriptions',
    'urn:li:dataHubPageModule:top_domains',
    'urn:li:dataHubPageModule:assets',
    'urn:li:dataHubPageModule:child_hierarchy',
    'urn:li:dataHubPageModule:data_products',
    'urn:li:dataHubPageModule:related_terms',
];

export const DEFAULT_TEMPLATE_URN = 'urn:li:dataHubPageTemplate:home_default_1';

export const CUSTOM_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.Link,
    DataHubPageModuleType.RichText,
    DataHubPageModuleType.Hierarchy,
    DataHubPageModuleType.AssetCollection,
];

export const LARGE_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.OwnedAssets,
    DataHubPageModuleType.Domains,
    DataHubPageModuleType.AssetCollection,
    DataHubPageModuleType.Hierarchy,
    DataHubPageModuleType.RichText,
    DataHubPageModuleType.Assets,
    DataHubPageModuleType.ChildHierarchy,
    DataHubPageModuleType.RelatedTerms,
    DataHubPageModuleType.DataProducts,
];

export const SMALL_MODULE_TYPES: DataHubPageModuleType[] = [DataHubPageModuleType.Link];

export const DEFAULT_TEMPLATE: PageTemplateFragment = {
    urn: DEFAULT_TEMPLATE_URN,
    type: EntityType.DatahubPageTemplate,
    properties: {
        visibility: {
            scope: PageTemplateScope.Global,
        },
        surface: {
            surfaceType: PageTemplateSurfaceType.HomePage,
        },
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:dataHubPageModule:your_assets',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Your Assets',
                            type: DataHubPageModuleType.OwnedAssets,
                            visibility: {
                                scope: PageModuleScope.Global,
                            },
                            params: {},
                        },
                    },
                    {
                        urn: 'urn:li:dataHubPageModule:top_domains',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Domains',
                            type: DataHubPageModuleType.Domains,
                            visibility: {
                                scope: PageModuleScope.Global,
                            },
                            params: {},
                        },
                    },
                ],
            },
        ],
    },
};
