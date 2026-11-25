import useFilters from '@app/search/utils/useFilters';
import { renderHook } from '@utils/test-utils/renderHook';

describe('useFilters', () => {
    it('should read a legacy env filter from the URL as origin', () => {
        const { result } = renderHook(() => useFilters({ filter_env: 'PROD' }));

        expect(result.current).toEqual([{ field: 'origin', condition: 'EQUAL', values: ['PROD'], negated: false }]);
    });
});
