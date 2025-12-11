/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEntityData } from '@app/entity/shared/EntityContext';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

export function useDocumentationPermission() {
    const { entityData } = useEntityData();
    const canUpdateGlossaryEntity = useCanUpdateGlossaryEntity();

    // Edit description permission
    const canEditDescription = !!entityData?.privileges?.canEditDescription;

    const canManageSummary = !!entityData?.privileges?.canManageAssetSummary;

    const hasDocumentationPermissions = canEditDescription || canUpdateGlossaryEntity || canManageSummary;

    return hasDocumentationPermissions;
}
