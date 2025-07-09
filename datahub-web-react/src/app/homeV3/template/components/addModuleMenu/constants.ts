import { IconNames } from '@components';

import { DataHubPageModule, DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mapping of module type to it's optional description override
// TODO: remove these description once descriptions in modules implemented
export const MODULE_TYPE_TO_DESCRIPTION_OVERRIDE: Map<DataHubPageModuleType, string> = new Map([
    [DataHubPageModuleType.AssetCollection, 'A curated list of assets of your choosing'],
    [DataHubPageModuleType.Domains, 'Most used domains in your organization'],
    [DataHubPageModuleType.Hierarchy, 'Top down view of assets'],
    [DataHubPageModuleType.Link, 'Choose links that are important'],
    [DataHubPageModuleType.OwnedAssets, 'Assets the current user owns'],
    [DataHubPageModuleType.RichText, 'Pin docs for your DataHub users'],
    [DataHubPageModuleType.SubscribedAssets, 'Assets the current user is subscribed to'],
]);

// Mapping of module type to it's icon
export const MODULE_TYPE_TO_ICON: Map<DataHubPageModuleType, IconNames> = new Map([
    [DataHubPageModuleType.AssetCollection, 'Stack'],
    [DataHubPageModuleType.Domains, 'Globe'],
    [DataHubPageModuleType.Hierarchy, 'SortAscending'],
    [DataHubPageModuleType.Link, 'LinkSimple'],
    [DataHubPageModuleType.OwnedAssets, 'Database'],
    [DataHubPageModuleType.RichText, 'TextT'],
    [DataHubPageModuleType.SubscribedAssets, 'Bell'],
]);

export const DEFAULT_MODULE_ICON = 'Database';

// TODO: Mocked default modules (should be replaced with the real calling of endpoint once it implemented)
export const MOCKED_CUSTOM_MODULES: DataHubPageModule[] = [
    {
        urn: 'urn:li:dataHubPageModule:your_assets_custom',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Assets (custom)',
            type: DataHubPageModuleType.OwnedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:your_subscriptions_custom',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Subscriptions (custom)',
            type: DataHubPageModuleType.SubscribedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:top_domains_custom',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Domains (custom)',
            type: DataHubPageModuleType.Domains,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
];

export const MOCKED_CUSTOM_LARGE_MODULES: DataHubPageModule[] = [
    {
        urn: 'urn:li:dataHubPageModule:your_assets',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Assets',
            type: DataHubPageModuleType.OwnedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:your_subscriptions',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Subscriptions',
            type: DataHubPageModuleType.SubscribedAssets,
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
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Domains',
            type: DataHubPageModuleType.Domains,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
];

export const MOCKED_ADMIN_CREATED_MODULES: DataHubPageModule[] = [
    {
        urn: 'urn:li:dataHubPageModule:your_assets_admin',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Assets (admin)',
            type: DataHubPageModuleType.OwnedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:your_subscriptions_admin',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Your Subscriptions (admin)',
            type: DataHubPageModuleType.SubscribedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:top_domains_admin',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Domains (admin)',
            type: DataHubPageModuleType.Domains,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
];
