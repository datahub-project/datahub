import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';

import { Dataset, FeatureFlagsConfig } from '@types';

const SNOWFLAKE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)';

function dataset(platform: string, typeName: string, sibling?: Dataset): Dataset {
    return {
        urn: `urn:li:dataset:(urn:li:dataPlatform:${platform},db.orders,PROD)`,
        name: 'orders',
        platform: { urn: `urn:li:dataPlatform:${platform}`, properties: { logoUrl: `${platform}.png` } },
        subTypes: { typeNames: [typeName] },
        siblingsSearch: sibling ? { searchResults: [{ entity: sibling }] } : undefined,
    } as unknown as Dataset;
}

const warehouseTable = dataset('snowflake', 'Table');
const flags = (overrides: Partial<FeatureFlagsConfig>) => overrides as FeatureFlagsConfig;

describe('DatasetEntity source merge in lineage', () => {
    const entity = new DatasetEntity();
    const lineageUrn = (data: Dataset, flagValues: Partial<FeatureFlagsConfig>) =>
        entity.getOverridePropertiesFromEntity(data, flags(flagValues)).lineageUrn;

    it.each([
        ['dbt', 'hideDbtSourceInLineage'],
        ['sqlmesh', 'hideSqlmeshSourceInLineage'],
    ] as const)('merges a %s source into its warehouse table when %s is on', (platform, flag) => {
        const source = dataset(platform, 'Source', warehouseTable);

        expect(lineageUrn(source, { [flag]: true })).toBe(SNOWFLAKE_URN);
        expect(lineageUrn(source, { [flag]: false })).toBeUndefined();
    });

    it("leaves each platform's sources to its own flag", () => {
        expect(
            lineageUrn(dataset('sqlmesh', 'Source', warehouseTable), { hideDbtSourceInLineage: true }),
        ).toBeUndefined();
        expect(
            lineageUrn(dataset('dbt', 'Source', warehouseTable), { hideSqlmeshSourceInLineage: true }),
        ).toBeUndefined();
    });

    it('never merges a SQLMesh model', () => {
        expect(
            lineageUrn(dataset('sqlmesh', 'Model', warehouseTable), { hideSqlmeshSourceInLineage: true }),
        ).toBeUndefined();
    });

    it('shows the SQLMesh icon on a warehouse table whose SQLMesh source is merged into it', () => {
        const table = dataset('snowflake', 'Table', dataset('sqlmesh', 'Source'));
        const props = entity.getOverridePropertiesFromEntity(table, flags({ hideSqlmeshSourceInLineage: true }));

        expect(props.lineageUrn).toBeUndefined();
        expect(props.lineageSiblingIcon).toBe('sqlmesh.png');
    });
});
