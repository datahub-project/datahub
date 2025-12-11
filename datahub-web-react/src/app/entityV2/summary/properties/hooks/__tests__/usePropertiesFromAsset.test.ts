/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';

describe('useAssetProperties', () => {
    it('should return undefined properties and loading as false', () => {
        const { result } = renderHook(() => useAssetProperties('test-urn'));

        expect(result.current.assetProperties).toBeUndefined();
        expect(result.current.loading).toBe(false);
    });
});
