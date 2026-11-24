import { useCallback, useMemo } from 'react';

import { LinkAttributes } from '@app/entityV2/shared/externalUrl/types';
import { sendClickExternalLinkAnalytics } from '@app/entityV2/shared/externalUrl/utils';
import { GenericEntityProperties } from '@app/entityV2/shared/types';
import { ExternalLinkType } from '@src/app/analytics';

export default function useExternalLinks(
    urn: string,
    genericEntityData: GenericEntityProperties | null,
): LinkAttributes[] {
    const links = useMemo(
        () => genericEntityData?.institutionalMemory?.elements || [],
        [genericEntityData?.institutionalMemory?.elements],
    );

    const sendAnalytics = useCallback(() => {
        sendClickExternalLinkAnalytics(urn, genericEntityData?.type, ExternalLinkType.Custom);
    }, [urn, genericEntityData]);

    return useMemo(
        () =>
            links
                .filter((link) => link.settings?.showInAssetPreview)
                .map((link) => ({
                    url: link.url,
                    label: link.label,
                    onClick: () => sendAnalytics(),
                })),
        [links, sendAnalytics],
    );
}
