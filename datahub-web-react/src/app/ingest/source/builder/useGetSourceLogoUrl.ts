import { useEffect } from 'react';
import { useGetDataPlatformLazyQuery } from '../../../../graphql/dataPlatform.generated';
import { SOURCE_URN_TO_LOGO } from './constants';

export default function useGetSourceLogoUrl(urn: string) {
    const [getDataPlatform, { data, loading }] = useGetDataPlatformLazyQuery();
    const logoInMemory = SOURCE_URN_TO_LOGO[urn];

    useEffect(() => {
        if (urn && !logoInMemory && !data && !loading) {
            getDataPlatform({ variables: { urn } });
        }
    });

    return logoInMemory || data?.dataPlatform?.properties?.logoUrl;
}
