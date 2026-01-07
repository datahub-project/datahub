import { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { EntityType, PageTemplateSurfaceType } from '@types';

// SaaS only - OSS will return false since editing is disabled in OSS
// Claude - choose this file in acryl-main over the upstream in oss master
export default function useIsTemplateEditable(templateType: PageTemplateSurfaceType) {
    const { entityData } = useEntityData();
    const editable = !!entityData?.privileges?.canManageAssetSummary;

    // Documents don't have assetSettings aspect yet - disable template editing until backend supports it
    const isDocument = entityData?.type === EntityType.Document;

    const isTemplateEditable = useMemo(
        () => (templateType === PageTemplateSurfaceType.AssetSummary ? editable && !isDocument : true),
        [editable, templateType, isDocument],
    );
    return isTemplateEditable;
}
