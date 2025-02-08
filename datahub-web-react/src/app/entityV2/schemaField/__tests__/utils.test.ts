import { getFieldPathFromSchemaFieldUrn, getSchemaFieldParentLink, getSourceUrnFromSchemaFieldUrn } from '../utils';

describe('schema field utils', () => {
    const schemaFieldUrn =
        'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD),profile_id)';

    it('should get a parent link for a schema field urn properly', () => {
        expect(getSchemaFieldParentLink(schemaFieldUrn)).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)/Columns?highlightedPath=profile_id',
        );
    });

    it('should get the source urn from a schema field urn properly', () => {
        expect(getSourceUrnFromSchemaFieldUrn(schemaFieldUrn)).toBe(
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)',
        );
    });

    it('should get the field path from a schema field urn properly', () => {
        expect(getFieldPathFromSchemaFieldUrn(schemaFieldUrn)).toBe('profile_id');
    });
});
