import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';

// TODO: add filters
export interface HierarchyForm {
    name: string;

    assetsType: typeof ASSET_TYPE_DOMAINS | typeof ASSET_TYPE_GLOSSARY;
    domainAssets?: string[];
    glossaryAssets?: string[];

    showRelatedEntities: boolean;
}

export interface HierarchyFormContextType {
    // Pass initial values as antd Form can't do that
    initialValues: HierarchyForm;
}
