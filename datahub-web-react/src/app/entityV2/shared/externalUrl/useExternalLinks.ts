/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useMemo } from 'react';

import { LinkAttributes } from '@app/entityV2/shared/externalUrl/types';
import { sendClickExternalLinkAnalytics } from '@app/entityV2/shared/externalUrl/utils';
import { ExternalLinkType } from '@src/app/analytics';
import { GenericEntityProperties } from '@src/app/entity/shared/types';

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
