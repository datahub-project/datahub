import {
    getFieldPathFromSchemaFieldUrn,
    getRawFieldPathFromSchemaFieldUrn,
    getSchemaFieldParentLink,
    getSourceUrnFromSchemaFieldUrn,
} from '@app/entityV2/schemaField/utils';

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

    // The raw path has to match a schemaMetadata field path, so it undoes exactly the escaping that
    // generateSchemaFieldUrn applies — including the comma that decodeURI would leave escaped.
    it('should undo the urn escaping of a field path', () => {
        const escapedFieldUrn = `urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD),[type=array%28string%2Cint%29].col_a)`;

        expect(getRawFieldPathFromSchemaFieldUrn(escapedFieldUrn)).toBe('[type=array(string,int)].col_a');
    });

    // Glossary terms carry schemaMetadata too, and their urns have no parentheses of their own.
    describe('parent urn without parentheses', () => {
        const glossaryTermFieldUrn = 'urn:li:schemaField:(urn:li:glossaryTerm:my_term,term_col_a)';

        it('should get the source urn from a schema field urn properly', () => {
            expect(getSourceUrnFromSchemaFieldUrn(glossaryTermFieldUrn)).toBe('urn:li:glossaryTerm:my_term');
        });

        it('should get the field path from a schema field urn properly', () => {
            expect(getFieldPathFromSchemaFieldUrn(glossaryTermFieldUrn)).toBe('term_col_a');
        });
    });
});
