import { describe, expect, it } from 'vitest';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import GlossaryNodeEntity from '@app/entityV2/glossaryNode/GlossaryNodeEntity';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';

describe('GlossaryNodeEntity tag support', () => {
    const entity = new GlossaryNodeEntity();

    it('declares TAGS as a supported capability', () => {
        expect(entity.supportedCapabilities().has(EntityCapabilityType.TAGS)).toBe(true);
    });

    it('includes SidebarTagsSection in the profile sidebar', () => {
        const sections = entity.getSidebarSections();
        expect(sections.some((section) => section.component === SidebarTagsSection)).toBe(true);
    });
});
