import { IconNames } from '@components';

import { ModuleInfo } from '@app/homeV3/modules/types';

import { DataHubPageModuleType } from '@types';

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

export const DEFAULT_MODULE_YOUR_ASSETS: ModuleInfo = {
    type: DataHubPageModuleType.OwnedAssets,
    name: 'Your Assets',
    description: MODULE_TYPE_TO_DESCRIPTION.get(DataHubPageModuleType.OwnedAssets),
    icon: MODULE_TYPE_TO_ICON.get(DataHubPageModuleType.OwnedAssets) ?? DEFAULT_MODULE_ICON,
    key: 'default_module_your_assets',
};

export const DEFAULT_MODULE_TOP_DOMAINS: ModuleInfo = {
    type: DataHubPageModuleType.Domains,
    name: 'Domains',
    description: MODULE_TYPE_TO_DESCRIPTION.get(DataHubPageModuleType.Domains),
    icon: MODULE_TYPE_TO_ICON.get(DataHubPageModuleType.Domains) ?? DEFAULT_MODULE_ICON,
    key: 'default_module_top_domains',
};

export const DEFAULT_MODULE_LINK: ModuleInfo = {
    type: DataHubPageModuleType.Link,
    name: 'Quick Link',
    description: MODULE_TYPE_TO_DESCRIPTION.get(DataHubPageModuleType.Link),
    icon: MODULE_TYPE_TO_ICON.get(DataHubPageModuleType.Link) ?? DEFAULT_MODULE_ICON,
    key: 'default_module_quick_link',
};

export const DEFAULT_MODULES: ModuleInfo[] = [
    DEFAULT_MODULE_YOUR_ASSETS,
    DEFAULT_MODULE_TOP_DOMAINS,
    // Links isn't supported yet
    // DEFAULT_MODULE_LINK,
];

export const ADD_MODULE_MENU_SECTION_CUSTOM_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.Link,
    DataHubPageModuleType.AssetCollection,
    DataHubPageModuleType.RichText,
    DataHubPageModuleType.Hierarchy,
];

export const ADD_MODULE_MENU_SECTION_CUSTOM_LARGE_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.Domains,
    DataHubPageModuleType.OwnedAssets,
];
