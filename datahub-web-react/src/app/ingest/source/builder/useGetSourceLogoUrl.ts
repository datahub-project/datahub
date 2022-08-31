import { useEffect } from 'react';
import { useGetDataPlatformLazyQuery } from '../../../../graphql/dataPlatform.generated';
import { CUSTOM, SOURCE_NAME_TO_LOGO } from './constants';

function generatePlatformUrn(platformName: string) {
    return `urn:li:dataPlatform:${platformName}`;
}

export default function useGetSourceLogoUrl(platformName: string) {
    const [getDataPlatform, { data, loading }] = useGetDataPlatformLazyQuery();
    const logoInMemory = SOURCE_NAME_TO_LOGO[platformName];

    useEffect(() => {
        if (!logoInMemory && platformName !== CUSTOM && !data && !loading) {
            getDataPlatform({ variables: { urn: generatePlatformUrn(platformName) } });
        }
    });

    return logoInMemory || data?.dataPlatform?.properties?.logoUrl;
}
