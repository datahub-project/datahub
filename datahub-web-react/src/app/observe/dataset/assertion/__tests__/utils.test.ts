import {
    FAILING_ASSERTION_TYPE_FILTER_FIELD,
    HAS_FAILING_ASSERTIONS_FILTER_FIELD,
} from '@app/observe/dataset/assertion/constants';
import {
    QueryParamDecoder,
    QueryParamEncoder,
    buildAssertionTypeFilters,
    compareListItems,
    getFilterFromQueryParams,
    setFilterToQueryParams,
} from '@app/observe/dataset/assertion/util';
import { UnionType } from '@app/search/utils/constants';

// Mock History and Location objects
const createMockLocation = (search: string) => ({
    search,
    pathname: '/test',
    hash: '',
    state: null,
    key: 'test',
});

const createMockHistory = () => {
    const calls: Array<{ method: string; args: any[] }> = [];

    const history = {
        push: (location: any) => calls.push({ method: 'push', args: [location] }),
        _calls: calls, // Add this for testing purposes
    } as any; // Use type assertion to avoid complex typing issues

    return history;
};

describe('utils', () => {
    describe('getFilterFromQueryParams', () => {
        interface TestFilter {
            name?: string;
            age?: number;
            active?: boolean;
        }

        const decoder: QueryParamDecoder<TestFilter> = {
            name: (value: string) => value || undefined,
            age: (value: string) => {
                const num = parseInt(value, 10);
                return Number.isNaN(num) ? undefined : num;
            },
            active: (value: string) => value === 'true',
        };

        it('should return default filter when no query params exist', () => {
            const defaultFilter: TestFilter = {
                name: 'default',
                age: 0,
                active: false,
            };
            const location = createMockLocation('');
            const result = getFilterFromQueryParams(decoder, defaultFilter, location);
            expect(result).toEqual(defaultFilter);
        });

        it('should decode query params correctly', () => {
            const defaultFilter: TestFilter = {
                name: 'default',
                age: 0,
                active: false,
            };
            const location = createMockLocation('?name=John&age=25&active=true');
            const result = getFilterFromQueryParams(decoder, defaultFilter, location);
            expect(result).toEqual({
                name: 'John',
                age: 25,
                active: true,
            });
        });

        it('should use default values when decoder returns undefined', () => {
            const defaultFilter: TestFilter = {
                name: 'default',
                age: 0,
                active: false,
            };
            const location = createMockLocation('?name=John&age=invalid&active=false');
            const result = getFilterFromQueryParams(decoder, defaultFilter, location);

            expect(result).toEqual({
                name: 'John',
                age: 0, // default value used because 'invalid' can't be parsed
                active: false,
            });

            // defaultFilter should remain unchanged (mutation bug is fixed)
            expect(defaultFilter).toEqual({
                name: 'default',
                age: 0,
                active: false,
            });
        });

        it('should handle partial query params', () => {
            const defaultFilter: TestFilter = {
                name: 'default',
                age: 0,
                active: false,
            };
            const location = createMockLocation('?name=Jane');
            const result = getFilterFromQueryParams(decoder, defaultFilter, location);

            expect(result).toEqual({
                name: 'Jane',
                age: 0,
                active: false,
            });

            // defaultFilter should remain unchanged
            expect(defaultFilter).toEqual({
                name: 'default',
                age: 0,
                active: false,
            });
        });

        it('should handle empty string values', () => {
            const defaultFilter: TestFilter = {
                name: 'default',
                age: 0,
                active: false,
            };
            const location = createMockLocation('?name=&age=30');
            const result = getFilterFromQueryParams(decoder, defaultFilter, location);

            expect(result).toEqual({
                name: 'default', // decoder returns undefined for empty string, so default is used
                age: 30,
                active: false,
            });

            // defaultFilter should remain unchanged
            expect(defaultFilter).toEqual({
                name: 'default',
                age: 0,
                active: false,
            });
        });
    });

    describe('setFilterToQueryParams', () => {
        interface TestFilter {
            name?: string;
            age?: number;
            active?: boolean;
        }

        const encoder: QueryParamEncoder<TestFilter> = {
            name: (value?: string) => value || '',
            age: (value?: number) => (value ? value.toString() : ''),
            active: (value?: boolean) => (value ? 'true' : ''),
        };

        it('should set query params correctly', () => {
            const filter: TestFilter = {
                name: 'John',
                age: 25,
                active: true,
            };
            const location = createMockLocation('');
            const history = createMockHistory();

            setFilterToQueryParams(filter, encoder, location, history);

            expect(history._calls).toEqual([{ method: 'push', args: [{ search: 'name=John&age=25&active=true' }] }]);
        });

        it('should remove params when encoder returns empty string', () => {
            const filter: TestFilter = {
                name: 'John',
                age: undefined,
                active: false,
            };
            const location = createMockLocation('?name=Jane&age=30&active=true');
            const history = createMockHistory();

            setFilterToQueryParams(filter, encoder, location, history);

            expect(history._calls).toEqual([{ method: 'push', args: [{ search: 'name=John' }] }]);
        });

        it('should preserve existing unrelated params', () => {
            const filter: TestFilter = {
                name: 'John',
            };
            const location = createMockLocation('?otherParam=value&name=Jane');
            const history = createMockHistory();

            setFilterToQueryParams(filter, encoder, location, history);

            expect(history._calls).toEqual([{ method: 'push', args: [{ search: 'otherParam=value&name=John' }] }]);
        });

        it('should handle empty filter (only processes keys that exist in filter)', () => {
            const filter: TestFilter = {};
            const location = createMockLocation('?name=John&age=25');
            const history = createMockHistory();

            setFilterToQueryParams(filter, encoder, location, history);

            expect(history._calls).toEqual([{ method: 'push', args: [{ search: 'name=John&age=25' }] }]);
        });
    });

    describe('buildAssertionTypeFilters', () => {
        it('should build OR filter when assertion types are provided', () => {
            const selectedAssertionTypes = ['type1', 'type2'];
            const result = buildAssertionTypeFilters(selectedAssertionTypes);

            expect(result).toEqual({
                unionType: UnionType.OR,
                filters: [
                    {
                        field: FAILING_ASSERTION_TYPE_FILTER_FIELD,
                        value: 'type1',
                    },
                    {
                        field: FAILING_ASSERTION_TYPE_FILTER_FIELD,
                        value: 'type2',
                    },
                ],
            });
        });

        it('should build AND filter when no assertion types are provided', () => {
            const result = buildAssertionTypeFilters(null);

            expect(result).toEqual({
                unionType: UnionType.AND,
                filters: [
                    {
                        field: HAS_FAILING_ASSERTIONS_FILTER_FIELD,
                        value: 'true',
                    },
                ],
            });
        });

        it('should build OR filter when empty array is provided (reveals truthy array bug)', () => {
            const result = buildAssertionTypeFilters([]);

            expect(result).toEqual({
                unionType: UnionType.OR,
                filters: [], // Empty array because selectedAssertionTypes.map([]) returns []
            });
        });
    });

    describe('compareListItems', () => {
        it('should return true for identical lists', () => {
            const list1 = ['a', 'b', 'c'];
            const list2 = ['a', 'b', 'c'];
            expect(compareListItems(list1, list2)).toBe(true);
        });

        it('should return true for identical lists in different order', () => {
            const list1 = ['a', 'b', 'c'];
            const list2 = ['c', 'a', 'b'];
            expect(compareListItems(list1, list2)).toBe(true);
        });

        it('should return false for lists with different lengths', () => {
            const list1 = ['a', 'b', 'c'];
            const list2 = ['a', 'b'];
            expect(compareListItems(list1, list2)).toBe(false);
        });

        it('should return false for lists with different items', () => {
            const list1 = ['a', 'b', 'c'];
            const list2 = ['a', 'b', 'd'];
            expect(compareListItems(list1, list2)).toBe(false);
        });

        it('should return true for empty lists', () => {
            const list1: string[] = [];
            const list2: string[] = [];
            expect(compareListItems(list1, list2)).toBe(true);
        });

        it('should handle duplicate items correctly', () => {
            const list1 = ['a', 'b', 'b', 'c'];
            const list2 = ['a', 'b', 'c', 'b'];
            expect(compareListItems(list1, list2)).toBe(true);
        });

        it('should return false when one list has duplicates and the other does not', () => {
            const list1 = ['a', 'b', 'c'];
            const list2 = ['a', 'b', 'b', 'c'];
            expect(compareListItems(list1, list2)).toBe(false);
        });
    });
});
