/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import useLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useLoader';
import { ChildrenLoaderType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

import { AndFilterInput } from '@types';

interface Props {
    parentValue: string;
    loadChildren: ChildrenLoaderType;
    loadRelatedEntities?: ChildrenLoaderType;
    relatedEntitiesOrFilters?: AndFilterInput[] | undefined;
}

export default function ChildLoader({
    parentValue,
    loadChildren,
    loadRelatedEntities,
    relatedEntitiesOrFilters,
}: Props) {
    useLoader(parentValue, loadChildren, loadRelatedEntities, relatedEntitiesOrFilters);

    return null;
}
