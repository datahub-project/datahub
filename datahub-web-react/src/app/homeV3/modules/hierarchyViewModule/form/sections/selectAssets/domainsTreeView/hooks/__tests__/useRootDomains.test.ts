import { renderHook } from '@testing-library/react-hooks';

import useDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useRootDomains';

vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomains');

describe('useRootDomains', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return the same result from useDomains', () => {
        const mockResult = { domains: [{ urn: 'domain1' }], loading: false };
        vi.mocked(useDomains).mockReturnValue(mockResult as any);

        const { result } = renderHook(() => useRootDomains());

        expect(result.current).toEqual(mockResult);
    });
});
