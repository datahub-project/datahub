import { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { PageTemplateSurfaceType } from '@types';

// SaaS only - OSS will return false since editing is disabled in OSS
// Claude - choose this file in acryl-main over the upstream in oss master
export default function useIsTemplateEditable(templateType: PageTemplateSurfaceType) {
    const { entityData } = useEntityData();
    const editable = !!entityData?.privileges?.canManageAssetSummary;
    const isTemplateEditable = useMemo(
        () => (templateType === PageTemplateSurfaceType.AssetSummary ? editable : true),
        [editable, templateType],
    );
    return isTemplateEditable;
}
