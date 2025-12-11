/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    ALL_EQUALS_OPERATOR,
    EQUALS_OPERATOR,
    EXISTS_OPERATOR,
    NOT_EQUALS_OPERATOR,
    NOT_EXISTS_OPERATOR,
    getOperatorOptionsForPredicate,
} from '@app/searchV2/filters/operator/operator';
import { FieldType, FilterOperatorType, FilterPredicate } from '@app/searchV2/filters/types';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '@src/app/search/utils/constants';
import { EntityType } from '@src/types.generated';

describe('operator', () => {
    const tagPredicate = {
        field: {
            field: 'tags',
            displayName: 'Tags',
            entityTypes: [EntityType.Tag],
            type: FieldType.ENUM,
        },
        operator: FilterOperatorType.EQUALS,
        values: [{ value: 'urn:li:tag:test1', count: 17, entity: null }],
        defaultValueOptions: [],
    } as FilterPredicate;

    const platformPredicate = {
        field: {
            field: 'platform',
            displayName: 'Platform',
            entityTypes: [],
            type: FieldType.ENUM,
        },
        operator: FilterOperatorType.EQUALS,
        values: [],
        defaultValueOptions: [],
    } as FilterPredicate;

    const entitySubtypePredicate = {
        field: {
            field: ENTITY_SUB_TYPE_FILTER_NAME,
            displayName: 'Type',
            entityTypes: [],
            type: FieldType.ENUM,
        },
        operator: FilterOperatorType.EQUALS,
        values: [],
        defaultValueOptions: [],
    } as FilterPredicate;

    const expectedEnumOptions = [EQUALS_OPERATOR, NOT_EQUALS_OPERATOR, EXISTS_OPERATOR, NOT_EXISTS_OPERATOR];

    const pluralExpectedEnumOptions = [
        EQUALS_OPERATOR,
        ALL_EQUALS_OPERATOR,
        NOT_EQUALS_OPERATOR,
        EXISTS_OPERATOR,
        NOT_EXISTS_OPERATOR,
    ];

    it('should return the expected operator options for a given enum field', () => {
        const options = getOperatorOptionsForPredicate(tagPredicate, false);
        expect(options).toMatchObject(expectedEnumOptions);
    });

    it('should return the expected operator options for a plural enum field', () => {
        const options = getOperatorOptionsForPredicate(tagPredicate, true);
        expect(options).toMatchObject(pluralExpectedEnumOptions);
    });

    it('should not include allEquals if filter is a platform filter', () => {
        const options = getOperatorOptionsForPredicate(platformPredicate, true);
        expect(options).toMatchObject(expectedEnumOptions);
    });

    it('should not include allEquals if filter is in an entity subtype filter', () => {
        const options = getOperatorOptionsForPredicate(entitySubtypePredicate, true);
        expect(options).toMatchObject(expectedEnumOptions);
    });
});
