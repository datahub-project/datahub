import { AssetType } from '@app/homeV3/modules/hierarchyViewModule/types';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

export interface HierarchyForm {
    name: string;

    assetsType: AssetType;
    domainAssets?: string[];
    glossaryAssets?: string[];

    showRelatedEntities: boolean;

    relatedEntitiesFilter?: LogicalPredicate | null;
}

export interface HierarchyFormContextType {
    // Pass initial values as antd Form can't do that
    initialValues: HierarchyForm;
}
