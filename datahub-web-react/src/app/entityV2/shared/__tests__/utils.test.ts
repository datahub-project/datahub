import { DataProduct, DatasetStatsSummary, EntityType } from '../../../../types.generated';
import {
    dictToQueryStringParams,
    getNumberWithOrdinal,
    encodeComma,
    decodeComma,
    notEmpty,
    truncate,
    singularizeCollectionName,
    getDataProduct,
    isOutputPort,
    getPlatformName,
    isListSubset,
    urlEncodeUrn,
    handleBatchError,
    getFineGrainedLineageWithSiblings,
    summaryHasStats,
} from '../utils';
import {
    mockEntityRelationShipResult,
    mockFineGrainedLineages1,
    mockRecord,
    mockSearchResult,
} from '../../../../Mocks';

describe('entity V2 utils test ->', () => {
    describe('dictToQueryStringParams ->', () => {
        it('should convert an object to string params', () => {
            expect(dictToQueryStringParams(mockRecord)).toEqual(
                'key1=value1&key2=true&key3=value2&key4=false&key5=value3&key6=true',
            );
        });
    });
    describe('urlEncodeUrn ->', () => {
        it('should encode URN string into URL format', () => {
            expect(urlEncodeUrn('urn#li:dataPlatformInstance?[urn%clickhouse,clickhousetestserver]')).toMatch(
                'urn%23li:dataPlatformInstance%3F%5Burn{{encoded_percent}}clickhouse,clickhousetestserver%5D',
            );
        });
    });
    describe('getNumberWithOrdinal ->', () => {
        it('should return number with appropriate suffix', () => {
            expect(getNumberWithOrdinal(3)).toBe('3rd');
            expect(getNumberWithOrdinal(10)).toBe('10th');
            expect(getNumberWithOrdinal(2)).toBe('2nd');
            expect(getNumberWithOrdinal(20)).toBe('20th');
            expect(getNumberWithOrdinal(53)).toBe('53rd');
        });
    });
    describe('encodeComma ->', () => {
        it('should replace commas with %2C', () => {
            expect(encodeComma('test,encode,commas')).toBe('test%2Cencode%2Ccommas');
        });
    });
    describe('decodeComma ->', () => {
        it('should replace %2C with commas', () => {
            expect(decodeComma('test%2Cencode%2Ccommas')).toBe('test,encode,commas');
        });
    });
    describe('notEmpty ->', () => {
        it('should return if value is null or undefined', () => {
            expect(notEmpty('test_value')).toBe(true);
            expect(notEmpty(null)).toBe(false);
            expect(notEmpty(undefined)).toBe(false);
        });
    });
    describe('truncate ->', () => {
        it('should truncate input text', () => {
            const largeText =
                'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.';

            expect(truncate(40, largeText)).toBe('Lorem ipsum dolor sit amet, consectetur ...');
        });
    });
    describe('singularizeCollectionName ->', () => {
        it('should return single collection name', () => {
            expect(singularizeCollectionName('posts')).toBe('post');
        });
    });
    describe('getPlatformName ->', () => {
        it('should return platform name with first letter capitalized', () => {
            const x: any = {
                urn: 'urn:li:dataPlatformInstance:(urn:li:dataPlatform:clickhouse,clickhousetestserver)',
                type: EntityType.DataPlatformInstance,
                instanceId: 'clickhousetestserver',
                platform: {
                    name: 'clickhouse',
                    type: 'DATA_PLATFORM',
                    urn: 'urn:li:dataPlatform:clickhouse',
                    properties: {
                        logoUrl: '/assets/platforms/clickhouselogo.png',
                    },
                },
            };
            expect(getPlatformName(x as any)).toBe('Clickhouse');
        });
    });
    describe('isListSubset ->', () => {
        it('should return if a list is a subset of another', () => {
            const arr1 = [1, 2, 3, 4];
            const arr2 = [1, 2, 3, 4, 5, 6, 7];
            expect(isListSubset(arr1, arr2)).toBe(true);
        });
    });
    describe('handleBatchError ->', () => {
        it('should return entities from EntityRelationshipsResult', () => {
            const urns = ['urn1', 'urn2'];
            const defaultMessage = { content: 'Default message', duration: 3 };
            test('should return custom message if urns length is greater than 1 and error code is 403', () => {
                const e = {
                    graphQLErrors: [{ extensions: { code: 403 } }],
                };
                const result = handleBatchError(urns, e, defaultMessage);
                expect(result).toEqual({
                    content:
                        'Your bulk edit selection included entities that you are unauthorized to update. The bulk edit being performed will not be saved.',
                    duration: 3,
                });
            });
            test('should return defaultMessage if urns length is not greater than 1', () => {
                const result = handleBatchError('urn', new Error(), defaultMessage);
                expect(result).toEqual(defaultMessage);
            });
            test('should return defaultMessage if urns length is greater than 1 but error code is not 403', () => {
                const e = {
                    graphQLErrors: [{ extensions: { code: 500 } }],
                };
                const result = handleBatchError(urns, e, defaultMessage);
                expect(result).toEqual(defaultMessage);
            });
        });
    });
    describe('getDataProduct ->', () => {
        it('should return entities from EntityRelationshipsResult', () => {
            expect(getDataProduct(mockEntityRelationShipResult)).toMatchObject(
                mockEntityRelationShipResult?.relationships[0].entity as DataProduct,
            );
        });
    });

    describe('getFineGrainedLineageWithSiblings ->', () => {
        describe('should put all of the fineGrainedLineages for a given entity and its siblings into one array so we have all of it in one place', () => {
            test('should return empty array if entityData is null', () => {
                const result = getFineGrainedLineageWithSiblings(null, vi.fn());
                expect(result).toEqual([]);
            });
            test('should return fineGrainedLineages from entityData if available', () => {
                const expectedResult = [
                    {
                        upstreams: [{ urn: 'urn:li:glossaryTerm:example.glossaryterm1', path: 'test_downstream1' }],
                        downstreams: [{ urn: 'urn:li:glossaryTerm:example.glossaryterm2', path: 'test_downstream2' }],
                    },
                ];
                const result = getFineGrainedLineageWithSiblings(mockFineGrainedLineages1, vi.fn());
                expect(result).toMatchObject(expectedResult);
            });
        });
    });
    describe('summaryHasStats ->', () => {
        it('should return whether summary has stats or not', () => {
            const mockPayload: DatasetStatsSummary = {
                __typename: 'DatasetStatsSummary',
                topUsersLast30Days: [{ urn: 'test_urn', type: EntityType.CorpUser, username: 'test_username' }],
            };
            expect(summaryHasStats(mockPayload)).toBe(true);
        });
    });
    describe('isOutputPort ->', () => {
        it('should return entities from EntityRelationshipsResult', () => {
            expect(isOutputPort(mockSearchResult)).toBe(true);
        });
    });
});
