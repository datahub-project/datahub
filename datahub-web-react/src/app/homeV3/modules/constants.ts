import { IconNames } from '@components';

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

export const CUSTOM_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.Link,
    DataHubPageModuleType.RichText,
    DataHubPageModuleType.Hierarchy,
    DataHubPageModuleType.AssetCollection,
];

export const ADD_MODULE_MENU_SECTION_CUSTOM_LARGE_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.Domains,
    DataHubPageModuleType.OwnedAssets,
    DataHubPageModuleType.AssetCollection,
];

export const DEFAULT_GLOBAL_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.OwnedAssets,
    DataHubPageModuleType.Domains,
];

export const LARGE_MODULE_TYPES: DataHubPageModuleType[] = [
    DataHubPageModuleType.OwnedAssets,
    DataHubPageModuleType.Domains,
    DataHubPageModuleType.AssetCollection,
    DataHubPageModuleType.Hierarchy,
    DataHubPageModuleType.RichText,
];

export const SMALL_MODULE_TYPES: DataHubPageModuleType[] = [DataHubPageModuleType.Link];
