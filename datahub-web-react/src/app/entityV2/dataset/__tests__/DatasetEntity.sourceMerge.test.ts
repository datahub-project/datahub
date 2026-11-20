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

    it('merges a dbt source into its warehouse table when hideDbtSourceInLineage is on', () => {
        const source = dataset('dbt', 'Source', warehouseTable);

        expect(lineageUrn(source, { hideDbtSourceInLineage: true })).toBe(SNOWFLAKE_URN);
        expect(lineageUrn(source, { hideDbtSourceInLineage: false })).toBeUndefined();
    });

    it('never merges a SQLMesh source or model', () => {
        expect(
            lineageUrn(dataset('sqlmesh', 'Source', warehouseTable), { hideDbtSourceInLineage: true }),
        ).toBeUndefined();
        expect(
            lineageUrn(dataset('sqlmesh', 'Model', warehouseTable), { hideDbtSourceInLineage: true }),
        ).toBeUndefined();
    });

    it('shows the dbt icon on a warehouse table whose dbt source is merged into it', () => {
        const table = dataset('snowflake', 'Table', dataset('dbt', 'Source'));
        const props = entity.getOverridePropertiesFromEntity(table, flags({ hideDbtSourceInLineage: true }));

        expect(props.lineageUrn).toBeUndefined();
        expect(props.lineageSiblingIcon).toBe('dbt.png');
    });
});
