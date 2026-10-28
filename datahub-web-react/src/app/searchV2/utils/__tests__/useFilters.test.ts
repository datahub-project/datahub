import { renderHook } from '@testing-library/react-hooks';

import useFilters from '@app/searchV2/utils/useFilters';

describe('useFilters', () => {
    it('should read a legacy env filter from the URL as origin', () => {
        const { result } = renderHook(() => useFilters({ filter_env: 'PROD' }));

        expect(result.current).toEqual([{ field: 'origin', condition: 'EQUAL', values: ['PROD'], negated: false }]);
    });
});
