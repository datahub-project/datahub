import { DBT_URN, SQLMESH_URN } from '@app/ingestV2/source/builder/constants';
import {
    TRANSFORMATION_PLATFORM_URNS,
    isSourceMergedIntoSibling,
    isSourceSubtype,
    isTransformationPlatform,
} from '@app/lineageV3/transformationPlatforms';

import { Dataset, EntityType, FeatureFlagsConfig } from '@types';

const DBT_DATASET = 'urn:li:dataset:(urn:li:dataPlatform:dbt,db.schema.model,PROD)';
const SQLMESH_DATASET = 'urn:li:dataset:(urn:li:dataPlatform:sqlmesh,db.schema.model,PROD)';
const SNOWFLAKE_DATASET = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)';

function dataset(platformUrn: string | undefined, typeNames?: string[] | null, withSubTypes = true): Dataset {
    return {
        urn: 'urn:li:dataset:x',
        type: EntityType.Dataset,
        platform: platformUrn ? { urn: platformUrn } : undefined,
        subTypes: withSubTypes ? { typeNames } : undefined,
    } as unknown as Dataset;
}

function flags(overrides: Partial<FeatureFlagsConfig> = {}): FeatureFlagsConfig {
    return { hideDbtSourceInLineage: false, hideSqlmeshSourceInLineage: false, ...overrides } as FeatureFlagsConfig;
}

describe('TRANSFORMATION_PLATFORM_URNS', () => {
    it('lists dbt and SQLMesh', () => {
        expect(TRANSFORMATION_PLATFORM_URNS).toEqual([DBT_URN, SQLMESH_URN]);
    });
});

describe('isTransformationPlatform', () => {
    it.each([
        ['dbt dataset', DBT_DATASET],
        ['SQLMesh dataset', SQLMESH_DATASET],
    ])('is true for a %s', (_, urn) => {
        expect(isTransformationPlatform({ urn, type: EntityType.Dataset })).toBe(true);
    });

    it('is true for a schema field of a SQLMesh dataset', () => {
        const urn = `urn:li:schemaField:(${SQLMESH_DATASET},col)`;
        expect(isTransformationPlatform({ urn, type: EntityType.SchemaField })).toBe(true);
    });

    it('is false for a warehouse dataset', () => {
        expect(isTransformationPlatform({ urn: SNOWFLAKE_DATASET, type: EntityType.Dataset })).toBe(false);
    });

    it('is false for a non-dataset type, even on a transformation platform', () => {
        expect(isTransformationPlatform({ urn: DBT_DATASET, type: EntityType.DataJob })).toBe(false);
    });

    it('is false when the urn is missing', () => {
        expect(isTransformationPlatform({ type: EntityType.Dataset })).toBe(false);
        expect(isTransformationPlatform({ urn: '', type: EntityType.Dataset })).toBe(false);
    });

    it('is false when the urn has no platform', () => {
        expect(isTransformationPlatform({ urn: 'urn:li:corpuser:someone', type: EntityType.Dataset })).toBe(false);
    });
});

describe('isSourceSubtype', () => {
    it.each([
        [null, false],
        [undefined, false],
        ['Source', true],
        ['Model', false],
    ])('isSourceSubtype(%s) is %s', (subtype, expected) => {
        expect(isSourceSubtype(subtype)).toBe(expected);
    });

    it('is false when called with no argument', () => {
        expect(isSourceSubtype()).toBe(false);
    });
});

describe('isSourceMergedIntoSibling', () => {
    it.each([
        ['dbt', DBT_URN, 'hideDbtSourceInLineage'],
        ['SQLMesh', SQLMESH_URN, 'hideSqlmeshSourceInLineage'],
    ] as const)('merges a %s source only when its flag is on', (_, platformUrn, flag) => {
        const source = dataset(platformUrn, ['Source']);
        expect(isSourceMergedIntoSibling(source, flags({ [flag]: true }))).toBe(true);
        expect(isSourceMergedIntoSibling(source, flags({ [flag]: false }))).toBe(false);
    });

    it("does not merge a source when only the other tool's flag is on", () => {
        expect(
            isSourceMergedIntoSibling(dataset(DBT_URN, ['Source']), flags({ hideSqlmeshSourceInLineage: true })),
        ).toBe(false);
        expect(
            isSourceMergedIntoSibling(dataset(SQLMESH_URN, ['Source']), flags({ hideDbtSourceInLineage: true })),
        ).toBe(false);
    });

    it('merges when Source is one of several subtypes', () => {
        expect(
            isSourceMergedIntoSibling(
                dataset(SQLMESH_URN, ['Table', 'Source']),
                flags({ hideSqlmeshSourceInLineage: true }),
            ),
        ).toBe(true);
    });

    const allOn = flags({ hideDbtSourceInLineage: true, hideSqlmeshSourceInLineage: true });

    it('does not merge a missing dataset', () => {
        expect(isSourceMergedIntoSibling(undefined, allOn)).toBe(false);
        expect(isSourceMergedIntoSibling(null, allOn)).toBe(false);
    });

    it('does not merge when flags are missing', () => {
        expect(isSourceMergedIntoSibling(dataset(DBT_URN, ['Source']))).toBe(false);
        expect(isSourceMergedIntoSibling(dataset(DBT_URN, ['Source']), undefined)).toBe(false);
    });

    it('does not merge a non-source subtype', () => {
        expect(isSourceMergedIntoSibling(dataset(DBT_URN, ['Model']), allOn)).toBe(false);
        expect(isSourceMergedIntoSibling(dataset(SQLMESH_URN, ['Model']), allOn)).toBe(false);
    });

    it('does not merge when subtypes are missing', () => {
        expect(isSourceMergedIntoSibling(dataset(DBT_URN, null), allOn)).toBe(false);
        expect(isSourceMergedIntoSibling(dataset(DBT_URN, undefined, false), allOn)).toBe(false);
    });

    it('does not merge a non-transformation platform', () => {
        expect(isSourceMergedIntoSibling(dataset('urn:li:dataPlatform:snowflake', ['Source']), allOn)).toBe(false);
    });

    it('does not merge a dataset with no platform', () => {
        expect(isSourceMergedIntoSibling(dataset(undefined, ['Source']), allOn)).toBe(false);
    });
});
