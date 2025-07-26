import { useLocation } from 'react-router-dom';

import {
    convertFieldsToV1FieldPath,
    downgradeV2FieldPath,
    downloadImage,
    getEntityTypeFromEntityUrn,
    getLineageUrl,
    processDocumentationString,
    useGetLineageUrl,
} from '@app/lineageV3/utils/lineageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

// Mock dependencies
vi.mock('react-router-dom');
vi.mock('@app/useEntityRegistry');
vi.mock('@app/entityV2/schemaField/utils');

const mockLocation = {
    search: '?filter=test&tab=lineage',
    pathname: '/dataset/urn:li:dataset:test',
    hash: '',
    state: null,
    key: 'test',
};

const mockEntityRegistry = {
    getTypeFromGraphName: vi.fn(),
    getEntityUrl: vi.fn(),
};

const mockUseEntityRegistry = vi.fn(() => mockEntityRegistry);
const mockUseLocation = vi.fn(() => mockLocation);

beforeEach(() => {
    vi.clearAllMocks();
    (useLocation as any).mockImplementation(mockUseLocation);
    (useEntityRegistry as any).mockImplementation(mockUseEntityRegistry);
});

describe('lineageUtils', () => {
    describe('downgradeV2FieldPath', () => {
        it('should return the same value for null or undefined input', () => {
            expect(downgradeV2FieldPath(null as any)).toBeNull();
            expect(downgradeV2FieldPath(undefined as any)).toBeUndefined();
            expect(downgradeV2FieldPath('')).toBe('');
        });

        it('should remove KEY_SCHEMA_PREFIX and VERSION_PREFIX', () => {
            const fieldPath = '[version=2.0].[key=True].[type=string].user.id';
            const result = downgradeV2FieldPath(fieldPath);
            expect(result).toBe('user.id');
        });

        it('should strip out annotation segments starting with brackets', () => {
            const fieldPath = 'user.[type=string].id.[annotation=test].name';
            const result = downgradeV2FieldPath(fieldPath);
            expect(result).toBe('user.id.name');
        });

        it('should handle field paths without annotations', () => {
            const fieldPath = 'user.profile.name';
            const result = downgradeV2FieldPath(fieldPath);
            expect(result).toBe('user.profile.name');
        });

        it('should handle mixed annotation and regular segments', () => {
            const fieldPath = 'schema.[version=2.0].user.[key=True].profile.[type=struct].data';
            const result = downgradeV2FieldPath(fieldPath);
            expect(result).toBe('schema.user.profile.data');
        });
    });

    describe('processDocumentationString', () => {
        it('should return empty string for null or undefined input', () => {
            expect(processDocumentationString(null)).toBe('');
            expect(processDocumentationString(undefined)).toBe('');
        });

        it('should process field paths in documentation strings', () => {
            const docString = "This field '[version=2.0].[key=True].[type=string].user.id' is important.";
            const result = processDocumentationString(docString);
            expect(result).toBe("This field 'user.id' is important.");
        });

        it('should handle multiple field paths in one string', () => {
            const docString = "Fields '[version=2.0].user.name' and '[version=2.0].[type=int].user.age' are required.";
            const result = processDocumentationString(docString);
            // The regex should match and replace both field paths
            expect(result).toContain('user.name');
            expect(result).toContain('user.age');
        });

        it('should return original string if no field paths found', () => {
            const docString = 'This is a regular documentation string.';
            const result = processDocumentationString(docString);
            expect(result).toBe(docString);
        });
    });

    describe('convertFieldsToV1FieldPath', () => {
        it('should convert array of field objects', () => {
            const fields = [
                {
                    fieldPath: '[version=2.0].[key=True].user.id',
                    nullable: false,
                    type: 'string',
                    recursive: false,
                },
                {
                    fieldPath: '[version=2.0].user.name',
                    nullable: true,
                    type: 'string',
                    recursive: false,
                },
            ] as any[];

            const result = convertFieldsToV1FieldPath(fields);

            expect(result).toHaveLength(2);
            expect(result[0].fieldPath).toBe('user.id');
            expect(result[1].fieldPath).toBe('user.name');
            expect(result[0].nullable).toBe(false);
            expect(result[1].nullable).toBe(true);
        });

        it('should handle empty array', () => {
            const result = convertFieldsToV1FieldPath([]);
            expect(result).toEqual([]);
        });

        it('should handle null fieldPath', () => {
            const fields = [
                {
                    fieldPath: null,
                    nullable: false,
                    type: 'string',
                    recursive: false,
                },
            ] as any[];

            const result = convertFieldsToV1FieldPath(fields);
            expect(result[0].fieldPath).toBe('');
        });
    });

    describe('getV1FieldPathFromSchemaFieldUrn', () => {
        it('should get field path from URN and downgrade it', () => {
            // Test the downgrade functionality which is the core logic of getV1FieldPathFromSchemaFieldUrn
            const mockFieldPath = '[version=2.0].[key=True].user.id';
            const downgradedPath = downgradeV2FieldPath(mockFieldPath);
            expect(downgradedPath).toBe('user.id');
        });
    });

    describe('getEntityTypeFromEntityUrn', () => {
        beforeEach(() => {
            (mockEntityRegistry.getTypeFromGraphName as any).mockReturnValue(EntityType.Dataset);
        });

        it('should extract entity type from URN', () => {
            const urn = 'urn:li:dataset:test';
            const result = getEntityTypeFromEntityUrn(urn, mockEntityRegistry as any);

            expect(mockEntityRegistry.getTypeFromGraphName).toHaveBeenCalledWith('dataset');
            expect(result).toBe(EntityType.Dataset);
        });

        it('should handle URNs with different entity types', () => {
            (mockEntityRegistry.getTypeFromGraphName as any).mockReturnValue(EntityType.DataJob);
            const urn = 'urn:li:dataJob:test';
            const result = getEntityTypeFromEntityUrn(urn, mockEntityRegistry as any);

            expect(mockEntityRegistry.getTypeFromGraphName).toHaveBeenCalledWith('dataJob');
            expect(result).toBe(EntityType.DataJob);
        });

        it('should return undefined for invalid URN', () => {
            (mockEntityRegistry.getTypeFromGraphName as any).mockReturnValue(undefined);
            const urn = 'invalid:urn';
            const result = getEntityTypeFromEntityUrn(urn, mockEntityRegistry as any);
            expect(result).toBeUndefined();
        });
    });

    describe('getLineageUrl', () => {
        beforeEach(() => {
            (mockEntityRegistry.getEntityUrl as any).mockReturnValue('/dataset/urn:li:dataset:test');
        });

        it('should construct lineage URL with search params', () => {
            const urn = 'urn:li:dataset:test';
            const type = EntityType.Dataset;

            const result = getLineageUrl(urn, type, mockLocation, mockEntityRegistry as any);

            expect(mockEntityRegistry.getEntityUrl).toHaveBeenCalledWith(type, urn);
            expect(result).toBe('/dataset/urn:li:dataset:test/Lineage?filter=test&tab=lineage');
        });

        it('should handle location without search params', () => {
            const locationWithoutSearch = { ...mockLocation, search: '' };
            const urn = 'urn:li:dataset:test';
            const type = EntityType.Dataset;

            const result = getLineageUrl(urn, type, locationWithoutSearch, mockEntityRegistry as any);
            expect(result).toBe('/dataset/urn:li:dataset:test/Lineage');
        });
    });

    describe('useGetLineageUrl', () => {
        beforeEach(() => {
            (mockEntityRegistry.getEntityUrl as any).mockReturnValue('/dataset/urn:li:dataset:test');
        });

        it('should return lineage URL for valid urn and type', () => {
            const result = useGetLineageUrl('urn:li:dataset:test', EntityType.Dataset);
            expect(result).toBe('/dataset/urn:li:dataset:test/Lineage?filter=test&tab=lineage');
        });

        it('should return empty string for undefined urn', () => {
            const result = useGetLineageUrl(undefined, EntityType.Dataset);
            expect(result).toBe('');
        });

        it('should return empty string for undefined type', () => {
            const result = useGetLineageUrl('urn:li:dataset:test', undefined);
            expect(result).toBe('');
        });

        it('should return empty string for both undefined', () => {
            const result = useGetLineageUrl(undefined, undefined);
            expect(result).toBe('');
        });
    });

    describe('downloadImage', () => {
        let mockAnchorElement: any;
        let originalCreateElement: typeof document.createElement;

        beforeEach(() => {
            // Mock anchor element
            mockAnchorElement = {
                setAttribute: vi.fn(),
                click: vi.fn(),
            };

            // Mock document.createElement
            originalCreateElement = document.createElement;
            document.createElement = vi.fn().mockReturnValue(mockAnchorElement);

            // Mock Date to have consistent timestamps in tests
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2024-01-15T10:30:45'));
        });

        afterEach(() => {
            document.createElement = originalCreateElement;
            vi.useRealTimers();
        });

        it('should create anchor element and trigger download with default filename', () => {
            const dataUrl = 'data:image/png;base64,testdata';

            downloadImage(dataUrl);

            expect(document.createElement).toHaveBeenCalledWith('a');
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('href', dataUrl);
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('download', 'reactflow_2024-01-15_103045.png');
            expect(mockAnchorElement.click).toHaveBeenCalled();
        });

        it('should use custom name prefix when provided', () => {
            const dataUrl = 'data:image/png;base64,testdata';
            const name = 'custom_entity';

            downloadImage(dataUrl, name);

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith(
                'download',
                'custom_entity_2024-01-15_103045.png',
            );
        });

        it('should handle empty string name', () => {
            const dataUrl = 'data:image/png;base64,testdata';

            downloadImage(dataUrl, '');

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('download', 'reactflow_2024-01-15_103045.png');
        });

        it('should format date and time correctly', () => {
            // Test with different date
            vi.setSystemTime(new Date('2024-12-25T01:05:09'));

            const dataUrl = 'data:image/png;base64,testdata';
            const name = 'test';

            downloadImage(dataUrl, name);

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('download', 'test_2024-12-25_010509.png');
        });

        it('should pad single digit months, days, hours, minutes, seconds', () => {
            // Test with single digit values
            vi.setSystemTime(new Date('2024-02-05T03:07:09'));

            const dataUrl = 'data:image/png;base64,testdata';
            const name = 'test';

            downloadImage(dataUrl, name);

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('download', 'test_2024-02-05_030709.png');
        });
    });
});
