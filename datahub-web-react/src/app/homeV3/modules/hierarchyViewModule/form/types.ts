import { AssetType } from '../types';

// TODO: add filters
export interface HierarchyForm {
    name: string;

    assetsType: AssetType;
    domainAssets?: string[];
    glossaryAssets?: string[];

    showRelatedEntities: boolean;
}

export interface HierarchyFormContextType {
    // Pass initial values as antd Form can't do that
    initialValues: HierarchyForm;
}
