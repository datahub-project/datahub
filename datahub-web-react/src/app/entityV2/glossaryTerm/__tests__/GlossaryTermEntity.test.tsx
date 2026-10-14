import { describe, expect, it } from 'vitest';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import { GlossaryTermEntity } from '@app/entityV2/glossaryTerm/GlossaryTermEntity';

describe('GlossaryTermEntity tag support', () => {
    const entity = new GlossaryTermEntity();

    it('declares TAGS as a supported capability', () => {
        expect(entity.supportedCapabilities().has(EntityCapabilityType.TAGS)).toBe(true);
    });

    it('includes SidebarTagsSection in the profile sidebar', () => {
        const sections = entity.getSidebarSections();
        expect(
            sections.some(
                (section) => (section.component as { displayName?: string }).displayName === 'SidebarTagsSection',
            ),
        ).toBe(true);
    });
});
