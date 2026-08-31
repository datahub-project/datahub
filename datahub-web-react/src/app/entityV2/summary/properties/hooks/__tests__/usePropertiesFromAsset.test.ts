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
