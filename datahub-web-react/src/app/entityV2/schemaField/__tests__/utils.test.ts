/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    getFieldPathFromSchemaFieldUrn,
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
});
