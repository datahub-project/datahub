import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { useCallback, useMemo } from 'react';
import { ExternalLinkType } from '@src/app/analytics';
import { sendClickExternalLinkAnalytics } from './utils';
import { LinkAttributes } from './types';

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
