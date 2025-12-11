/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
