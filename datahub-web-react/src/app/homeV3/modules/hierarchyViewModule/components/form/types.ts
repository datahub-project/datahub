/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
