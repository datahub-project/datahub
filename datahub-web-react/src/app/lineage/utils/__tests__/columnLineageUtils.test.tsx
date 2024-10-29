import {
    decodeSchemaField,
    encodeSchemaField,
    getFieldPathFromSchemaFieldUrn,
    getPopulatedColumnsByUrn,
    getSourceUrnFromSchemaFieldUrn,
} from '../columnLineageUtils';
import { dataJob1, dataset1, dataset2 } from '../../../../Mocks';
import { FetchedEntity } from '../../types';
import { FineGrainedLineage, SchemaFieldDataType } from '../../../../types.generated';

describe('getSourceUrnFromSchemaFieldUrn', () => {
    it('should get the source urn for a chart schemaField', () => {
        const schemaFieldUrn = 'urn:li:schemaField:(urn:li:chart:(looker,dashboard_elements.1),goal)';
        const sourceUrn = getSourceUrnFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('urn:li:chart:(looker,dashboard_elements.1)');
    });

    it('should get the source urn for a dataset schemaField', () => {
        const schemaFieldUrn =
            'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD),user_name)';
        const sourceUrn = getSourceUrnFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD)');
    });

    it('should get the source urn for a nested schemaField', () => {
        const schemaFieldUrn =
            'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD),user.name.test)';
        const sourceUrn = getSourceUrnFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD)');
    });
});

describe('getFieldPathFromSchemaFieldUrn', () => {
    it('should get the fieldPath from a chart schemaField urn', () => {
        const schemaFieldUrn = 'urn:li:schemaField:(urn:li:chart:(looker,dashboard_elements.1),goal)';
        const sourceUrn = getFieldPathFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('goal');
    });

    it('should get the fieldPath for a dataset schemaField', () => {
        const schemaFieldUrn =
            'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD),user_name)';
        const sourceUrn = getFieldPathFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('user_name');
    });

    it('should get the fieldPath for a nested schemaField', () => {
        const schemaFieldUrn =
            'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD),user.name.test)';
        const sourceUrn = getFieldPathFromSchemaFieldUrn(schemaFieldUrn);
        expect(sourceUrn).toBe('user.name.test');
    });
});

describe('decodeSchemaField', () => {
    it('should decode a schemaField path when it has encoded reserved characters', () => {
        const decodedSchemaField = decodeSchemaField('Test%2C test%2C test %28and test%29');
        expect(decodedSchemaField).toBe('Test, test, test (and test)');
    });

    it('should return a regular schemaField path when it was not encoded', () => {
        const schemaField = 'user.name';
        const decodedSchemaField = decodeSchemaField(schemaField);
        expect(decodedSchemaField).toBe(schemaField);
    });
});

describe('encodeSchemaField', () => {
    it('should encode a schemaField path when it has encoded reserved characters', () => {
        const encodedSchemaField = encodeSchemaField('Test, test, test (and test)');
        expect(encodedSchemaField).toBe('Test%2C test%2C test %28and test%29');
    });

    it('should return a regular schemaField path when it does not have reserved characters', () => {
        const schemaField = 'user.name';
        const encodedSchemaField = encodeSchemaField(schemaField);
        expect(encodedSchemaField).toBe(schemaField);
    });

    it('should encode a decoded schemaField that we generate', () => {
        const schemaField = 'Adults, men (% of pop)';
        const encodedSchemaField = encodeSchemaField(schemaField);
        const decodedSchemaField = decodeSchemaField(encodedSchemaField);
        expect(encodedSchemaField).toBe('Adults%2C men %28% of pop%29');
        expect(decodedSchemaField).toBe(schemaField);
    });
});

describe('getPopulatedColumnsByUrn', () => {
    it('should update columns by urn with data job fine grained data so that the data job appears to have the upstream and downstream columns', () => {
        const dataJobWithCLL = {
            ...dataJob1,
            name: '',
            fineGrainedLineages: [
                {
                    upstreams: [{ urn: dataset1.urn, path: 'test1' }],
                    downstreams: [{ urn: dataset2.urn, path: 'test2' }],
                },
                {
                    upstreams: [{ urn: dataset1.urn, path: 'test3' }],
                    downstreams: [{ urn: dataset2.urn, path: 'test4' }],
                },
            ] as FineGrainedLineage[],
        };
        const fetchedEntities = new Map([[dataJobWithCLL.urn, dataJobWithCLL as FetchedEntity]]);
        const columnsByUrn = getPopulatedColumnsByUrn({}, fetchedEntities);

        expect(columnsByUrn).toMatchObject({
            [dataJobWithCLL.urn]: [
                {
                    fieldPath: 'test1',
                    nullable: false,
                    recursive: false,
                    type: SchemaFieldDataType.String,
                },
                {
                    fieldPath: 'test2',
                    nullable: false,
                    recursive: false,
                    type: SchemaFieldDataType.String,
                },
                {
                    fieldPath: 'test3',
                    nullable: false,
                    recursive: false,
                    type: SchemaFieldDataType.String,
                },
                {
                    fieldPath: 'test4',
                    nullable: false,
                    recursive: false,
                    type: SchemaFieldDataType.String,
                },
            ],
        });
    });
});
