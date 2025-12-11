/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { CUSTOM, PLATFORM_URN_TO_LOGO, SOURCE_TO_PLATFORM_URN } from '@app/ingestV2/source/builder/constants';

import { useGetDataPlatformLazyQuery } from '@graphql/dataPlatform.generated';

function generatePlatformUrn(platformName: string) {
    return `urn:li:dataPlatform:${platformName}`;
}

export default function useGetSourceLogoUrl(sourceName: string) {
    const [getDataPlatform, { data, loading }] = useGetDataPlatformLazyQuery();

    let platformUrn = SOURCE_TO_PLATFORM_URN[sourceName];
    if (!platformUrn) {
        platformUrn = generatePlatformUrn(sourceName);
    }
    const logoInMemory = PLATFORM_URN_TO_LOGO[platformUrn];

    useEffect(() => {
        if (!logoInMemory && sourceName !== CUSTOM && !data && !loading) {
            getDataPlatform({ variables: { urn: platformUrn } });
        }
    });

    return logoInMemory || (data?.dataPlatform?.properties?.logoUrl as string);
}
