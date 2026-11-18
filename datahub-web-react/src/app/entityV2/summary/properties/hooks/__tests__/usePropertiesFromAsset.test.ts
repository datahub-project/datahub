import { describe, expect, it } from 'vitest';

import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';
import { renderHook } from '@utils/test-utils/renderHook';

describe('useAssetProperties', () => {
    it('should return undefined properties and loading as false', () => {
        const { result } = renderHook(() => useAssetProperties('test-urn'));

        expect(result.current.assetProperties).toBeUndefined();
        expect(result.current.loading).toBe(false);
    });
});
