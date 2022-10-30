import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from '../columnLineageUtils';

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
