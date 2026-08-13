import { describe, expect, it, vi } from 'vitest';

import { SchemaFieldEntity } from '@app/entityV2/schemaField/SchemaFieldEntity';

import { EntityType, SchemaFieldEntity as SchemaField } from '@types';

vi.mock('@app/globalEntityRegistryV2', () => ({
    default: {
        getGenericEntityProperties: () => ({ name: 'my_dataset' }),
    },
}));

describe('SchemaFieldEntity', () => {
    describe('getLineageVizConfig', () => {
        it('decodes percent-encoded characters in the field path for the lineage node name', () => {
            const entity = {
                urn: 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_table,PROD),Revenue %28Net%29)',
                type: EntityType.SchemaField,
                fieldPath: 'Revenue %28Net%29',
                parent: {
                    urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_table,PROD)',
                    type: EntityType.Dataset,
                },
            } as unknown as SchemaField;

            const config = new SchemaFieldEntity().getLineageVizConfig(entity);

            expect(config.name).toBe('Revenue (Net)');
            expect(config.expandedName).toBe('my_dataset.Revenue (Net)');
        });
    });
});
