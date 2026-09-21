import { valueMatches, valueUrn } from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValueList';
import { ValueColumnData } from '@app/entityV2/shared/tabs/Properties/types';
import EntityRegistry from '@src/app/entityV2/EntityRegistry';

import { Entity, EntityType } from '@types';

const registry = {
    getDisplayName: (_: EntityType, entity: Entity) => `Display ${entity.urn}`,
} as unknown as EntityRegistry;

const plain: ValueColumnData = { value: 'EU_WEST', entity: null };
const urnString: ValueColumnData = { value: 'urn:li:glossaryTerm:abc', entity: null };
const resolved: ValueColumnData = {
    value: 'urn:li:glossaryTerm:abc',
    entity: { urn: 'urn:li:glossaryTerm:abc', type: EntityType.GlossaryTerm } as Entity,
};

describe('StructuredPropertyValueList helpers', () => {
    it('finds the URN behind a value from the resolved entity or a bare URN string', () => {
        expect(valueUrn(resolved)).toBe('urn:li:glossaryTerm:abc');
        expect(valueUrn(urnString)).toBe('urn:li:glossaryTerm:abc');
        expect(valueUrn(plain)).toBeUndefined();
    });

    it('matches on the raw value case-insensitively and lets an empty query through', () => {
        expect(valueMatches(plain, 'eu_w', registry)).toBe(true);
        expect(valueMatches(plain, 'us', registry)).toBe(false);
        expect(valueMatches(plain, '  ', registry)).toBe(true);
    });

    it('matches entity values on their display name', () => {
        expect(valueMatches(resolved, 'display urn:li:glossaryterm:abc', registry)).toBe(true);
        expect(valueMatches(resolved, 'nothing', registry)).toBe(false);
    });
});
