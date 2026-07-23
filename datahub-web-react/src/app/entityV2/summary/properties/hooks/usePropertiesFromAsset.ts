import { AssetProperty } from '@app/entityV2/summary/properties/types';

interface Response {
    assetProperties: AssetProperty[] | undefined;
    loading: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function useAssetProperties(entityUrn: string): Response {
    // TODO: implement loading and transformation of asset properties
    return {
        assetProperties: undefined,
        loading: false,
    };
}
