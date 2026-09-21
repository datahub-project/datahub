import {
    selectVisibleValues,
    valueMatches,
    valueUrn,
} from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValueList';
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

    describe('selectVisibleValues', () => {
        const many: ValueColumnData[] = Array.from({ length: 300 }, (_, i) => ({
            value: `choice_${String(i).padStart(3, '0')}`,
            entity: null,
        }));

        it('returns everything when neither filter is set', () => {
            expect(selectVisibleValues(many, '', undefined, registry)).toHaveLength(300);
        });

        it('narrows by the page-level search so a match past the first page is still visible', () => {
            const visible = selectVisibleValues(many, '', 'choice_25', registry);
            expect(visible.map((v) => v.value)).toEqual([
                'choice_250',
                'choice_251',
                'choice_252',
                'choice_253',
                'choice_254',
                'choice_255',
                'choice_256',
                'choice_257',
                'choice_258',
                'choice_259',
            ]);
        });

        it('applies the list filter and the page-level search together', () => {
            expect(selectVisibleValues(many, '_25', 'choice_259', registry).map((v) => v.value)).toEqual([
                'choice_259',
            ]);
            expect(selectVisibleValues(many, 'choice_001', 'choice_259', registry)).toHaveLength(0);
        });
    });
});
