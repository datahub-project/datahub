import { describe, expect, it, vi } from 'vitest';

import { BrowseSortOrder } from '@app/searchV2/sidebar/BrowseSortContext';
import { getBrowseResultGroupLabel, sortBrowseResultGroups } from '@app/searchV2/sidebar/browseSortUtils';
import type { EntityRegistry } from '@src/entityRegistryContext';
import type { BrowseResultGroupV2 } from '@types';
import { EntityType } from '@types';

const mockEntityRegistry = {
    getDisplayName: vi.fn((type, entity) => {
        if (type === EntityType.Container) {
            return entity.properties?.name ?? entity.urn;
        }

        return entity.name ?? entity.urn;
    }),
} as unknown as EntityRegistry;

function buildGroup(name: string, entityName?: string): BrowseResultGroupV2 {
    return {
        name,
        count: 1,
        hasSubGroups: false,
        entity: entityName
            ? ({
                  urn: `urn:li:container:${entityName}`,
                  type: EntityType.Container,
                  properties: { name: entityName },
              } as any)
            : null,
    };
}

describe('browseSortUtils', () => {
    it('uses the entity display name when a browse group has an entity', () => {
        expect(getBrowseResultGroupLabel(buildGroup('ignored-name', 'Visible Label'), mockEntityRegistry)).toBe(
            'Visible Label',
        );
    });

    it('falls back to the raw browse group name when no entity exists', () => {
        expect(getBrowseResultGroupLabel(buildGroup('plain-folder'), mockEntityRegistry)).toBe('plain-folder');
    });

    it('sorts browse groups A-Z using the visible label', () => {
        const sorted = sortBrowseResultGroups(
            [buildGroup('z-folder'), buildGroup('internal-b', 'Beta'), buildGroup('a-folder')],
            BrowseSortOrder.ALPHABETICAL_ASC,
            mockEntityRegistry,
        );

        expect(sorted.map((group) => getBrowseResultGroupLabel(group, mockEntityRegistry))).toEqual([
            'a-folder',
            'Beta',
            'z-folder',
        ]);
    });

    it('sorts browse groups Z-A using the visible label', () => {
        const sorted = sortBrowseResultGroups(
            [buildGroup('z-folder'), buildGroup('internal-b', 'Beta'), buildGroup('a-folder')],
            BrowseSortOrder.ALPHABETICAL_DESC,
            mockEntityRegistry,
        );

        expect(sorted.map((group) => getBrowseResultGroupLabel(group, mockEntityRegistry))).toEqual([
            'z-folder',
            'Beta',
            'a-folder',
        ]);
    });

    it('preserves backend order for Recently Used', () => {
        const original = [buildGroup('c-folder'), buildGroup('a-folder'), buildGroup('b-folder')];

        const sorted = sortBrowseResultGroups(original, BrowseSortOrder.RECENTLY_USED, mockEntityRegistry);

        expect(sorted.map((group) => group.name)).toEqual(['c-folder', 'a-folder', 'b-folder']);
    });
});
