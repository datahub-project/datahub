/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';

import { EntityType } from '@types';

export function useCanUpdateGlossaryEntity() {
    const { entityData, entityType } = useEntityData();
    const user = useUserContext();

    const canManageGlossaries = !!user?.platformPrivileges?.manageGlossaries;
    const canManageChildren = !!entityData?.privileges?.canManageChildren;

    // Manage Glossary or manage children permission for Glossary terms and Glossary nodes
    const canUpdateGlossaryEntity =
        (entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm) &&
        (canManageGlossaries || canManageChildren);

    return canUpdateGlossaryEntity;
}
