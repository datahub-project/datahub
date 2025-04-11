import {
    decodeComma,
    decodeUrn,
    dictToQueryStringParams,
    encodeComma,
    getDataProduct,
    getNumberWithOrdinal,
    getPlatformName,
    handleBatchError,
    isListSubset,
    notEmpty,
    singularizeCollectionName,
    summaryHasStats,
    truncate,
    urlEncodeUrn,
} from '../../utils';

describe('dictToQueryStringParams', () => {
    it('should convert dictionary to query string', () => {
        const params = { key1: 'value1', key2: true };
        const queryString = dictToQueryStringParams(params);
        expect(queryString).toBe('key1=value1&key2=true');
    });
});

describe('urlEncodeUrn', () => {
    it('should encode URN properly', () => {
        const urn = 'urn:li:glossaryTerm?cb326461-7b05-4f07-9069-e139dd92f5ee%';
        const encodedUrn = urlEncodeUrn(urn);
        expect(encodedUrn).toBe('urn:li:glossaryTerm%3Fcb326461-7b05-4f07-9069-e139dd92f5ee{{encoded_percent}}');
    });
});

describe('decodeUrn', () => {
    it('should return the correct decoded URN', () => {
        const encodedUrn = 'urn%3Aexample%3Aentity';
        const expectedUrn = 'urn:example:entity';
        expect(decodeUrn(encodedUrn)).toEqual(expectedUrn);
    });
});

describe('getNumberWithOrdinal', () => {
    it('should return the correct number with ordinal suffix', () => {
        expect(getNumberWithOrdinal(1)).toEqual('1st');
        expect(getNumberWithOrdinal(2)).toEqual('2nd');
        expect(getNumberWithOrdinal(3)).toEqual('3rd');
        expect(getNumberWithOrdinal(4)).toEqual('4th');
    });
});

describe('encodeComma', () => {
    it('should return the correct encoded string with commas replaced', () => {
        const str = 'comma,separated,string';
        const expectedEncodedString = 'comma%2Cseparated%2Cstring';
        expect(encodeComma(str)).toEqual(expectedEncodedString);
    });
});

describe('decodeComma', () => {
    it('should return the correct decoded string with commas restored', () => {
        const encodedStr = 'comma%2Cseparated%2Cstring';
        const expectedDecodedString = 'comma,separated,string';
        expect(decodeComma(encodedStr)).toEqual(expectedDecodedString);
    });
});

describe('notEmpty', () => {
    it('should return true for not empty values', () => {
        expect(notEmpty('value')).toBe(true);
        expect(notEmpty(0)).toBe(true);
        expect(notEmpty(false)).toBe(true);
    });

    it('should return false for empty or null/undefined values', () => {
        expect(notEmpty(null)).toBe(false);
        expect(notEmpty(undefined)).toBe(false);
    });
});

describe('truncate', () => {
    it('should truncate string if it exceeds length', () => {
        const input = 'This is a long string';
        const length = 10;
        const expectedOutput = 'This is a ...';
        expect(truncate(length, input)).toEqual(expectedOutput);
    });

    it('should return original string if length is greater or equal to string length', () => {
        const input = 'Short';
        const length = 10;
        expect(truncate(length, input)).toEqual(input);
    });

    it('should return empty string if input is null or undefined', () => {
        expect(truncate(10, null)).toEqual('');
        expect(truncate(10, undefined)).toEqual('');
    });
});

describe('singularizeCollectionName', () => {
    it('should singularize plural collection name', () => {
        const pluralName = 'Entities';
        const expectedSingularName = 'Entitie';
        expect(singularizeCollectionName(pluralName)).toEqual(expectedSingularName);
    });

    it('should return the same name if it cannot be singularized', () => {
        const singularName = 'Entity';
        expect(singularizeCollectionName(singularName)).toEqual(singularName);
    });
});

describe('getPlatformName', () => {
    it('should return platform name or display name if available', () => {
        const entityData: any = {
            platform: {
                properties: {
                    displayName: 'Teradata',
                },
                name: 'teradata',
            },
        };
        expect(getPlatformName(entityData)).toBe('Teradata');
    });
});

describe('isListSubset', () => {
    it('should return true if l1 is a subset of l2', () => {
        const l1 = [1, 2];
        const l2 = [1, 2, 3, 4];
        expect(isListSubset(l1, l2)).toBe(true);
    });

    it('should return false if l1 is not a subset of l2', () => {
        const l1 = [1, 5];
        const l2 = [1, 2, 3, 4];
        expect(isListSubset(l1, l2)).toBe(false);
    });
});

describe('handleBatchError', () => {
    it('should return custom error message for unauthorized bulk edit', () => {
        const urns = ['urn1', 'urn2'];
        const e = { graphQLErrors: [{ extensions: { code: 403 } }] };
        const defaultMessage = 'Default error message';
        const expectedErrorMessage = {
            content:
                'Your bulk edit selection included entities that you are unauthorized to update. The bulk edit being performed will not be saved.',
            duration: 3,
        };
        expect(handleBatchError(urns, e, defaultMessage)).toEqual(expectedErrorMessage);
    });

    it('should return default message for other errors', () => {
        const urns = ['urn1'];
        const e = { graphQLErrors: [{ extensions: { code: 500 } }] };
        const defaultMessage = 'Default error message';
        expect(handleBatchError(urns, e, defaultMessage)).toEqual(defaultMessage);
    });
});

describe('getDataProduct', () => {
    it('should return data product properly if relationships exist', () => {
        const dataProductResult: any = {
            relationships: [{ entity: { id: 1, name: 'Data Product' } }],
        };

        const result = getDataProduct(dataProductResult);

        expect(result).toEqual({ id: 1, name: 'Data Product' });
    });

    it('should return null if no relationships exist', () => {
        const dataProductResult = {
            relationships: [],
        };

        const result = getDataProduct(dataProductResult);

        expect(result).toBeNull();
    });
});

describe('summaryHasStats', () => {
    it('should return true if statsSummary has stats', () => {
        const statsSummary = {
            queryCountLast30Days: 10,
        };

        const result = summaryHasStats(statsSummary);

        expect(result).toBe(true);
    });

    it('should return false if statsSummary does not have stats', () => {
        const statsSummary = {
            queryCountLast30Days: null,
        };

        const result = summaryHasStats(statsSummary);

        expect(result).toBe(false);
    });

    it('should return false if statsSummary is null or undefined', () => {
        const result1 = summaryHasStats(null);
        const result2 = summaryHasStats(undefined);

        expect(result1).toBe(false);
        expect(result2).toBe(false);
    });
});
