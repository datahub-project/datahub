import {
    ALL_EQUALS_OPERATOR,
    EQUALS_OPERATOR,
    EXISTS_OPERATOR,
    NOT_EQUALS_OPERATOR,
    NOT_EXISTS_OPERATOR,
    WITHIN_OPERATOR,
    convertBackendToFrontendOperatorType,
    convertFrontendToBackendOperatorType,
    getOperatorOptionsForPredicate,
} from '@app/searchV2/filters/operator/operator';
import { FieldType, FilterOperatorType, FilterPredicate } from '@app/searchV2/filters/types';
import {
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    PARENT_DOCUMENT_FILTER_NAME,
} from '@src/app/search/utils/constants';
import { EntityType, FilterOperator } from '@src/types.generated';

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

    const domainsPredicate = {
        field: {
            field: DOMAINS_FILTER_NAME,
            displayName: 'Domain',
            entityTypes: [EntityType.Domain],
            type: FieldType.ENTITY,
        },
        operator: FilterOperatorType.EQUALS,
        values: [{ value: 'urn:li:domain:marketing', entity: null }],
        defaultValueOptions: [],
    } as FilterPredicate;

    const containerPredicate = {
        field: {
            field: CONTAINER_FILTER_NAME,
            displayName: 'Container',
            entityTypes: [EntityType.Container],
            type: FieldType.ENTITY,
        },
        operator: FilterOperatorType.EQUALS,
        values: [{ value: 'urn:li:container:abc', entity: null }],
        defaultValueOptions: [],
    } as FilterPredicate;

    const parentDocumentPredicate = {
        field: {
            field: PARENT_DOCUMENT_FILTER_NAME,
            displayName: 'Parent Document',
            entityTypes: [EntityType.Document],
            type: FieldType.ENTITY,
        },
        operator: FilterOperatorType.EQUALS,
        values: [{ value: 'urn:li:document:parent', entity: null }],
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

    it('should put Within first for domains filters', () => {
        const options = getOperatorOptionsForPredicate(domainsPredicate, false);
        expect(options[0]).toMatchObject(WITHIN_OPERATOR);
        expect(options.map((o) => o.type)).toEqual([
            FilterOperatorType.WITHIN,
            FilterOperatorType.EQUALS,
            FilterOperatorType.NOT_EQUALS,
            FilterOperatorType.EXISTS,
            FilterOperatorType.NOT_EXISTS,
        ]);
    });

    it('should put Within first for container filters', () => {
        const options = getOperatorOptionsForPredicate(containerPredicate, false);
        expect(options[0]).toMatchObject(WITHIN_OPERATOR);
    });

    it('should put Within first for parentDocument filters', () => {
        const options = getOperatorOptionsForPredicate(parentDocumentPredicate, false);
        expect(options[0]).toMatchObject(WITHIN_OPERATOR);
    });

    it('should not include Within for non-hierarchical entity filters', () => {
        const options = getOperatorOptionsForPredicate(tagPredicate, false);
        expect(options.map((o) => o.type)).not.toContain(FilterOperatorType.WITHIN);
    });

    it('should map Within to DescendantsIncl and back', () => {
        expect(convertFrontendToBackendOperatorType(FilterOperatorType.WITHIN)).toEqual({
            operator: FilterOperator.DescendantsIncl,
            negated: false,
        });
        expect(
            convertBackendToFrontendOperatorType({
                operator: FilterOperator.DescendantsIncl,
                negated: false,
            }),
        ).toBe(FilterOperatorType.WITHIN);
    });
});
