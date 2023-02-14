import { useEffect } from 'react';
import { useGetDataPlatformLazyQuery } from '../../../../graphql/dataPlatform.generated';
import { CUSTOM, SOURCE_TO_PLATFORM_URN, PLATFORM_URN_TO_LOGO } from './constants';

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
