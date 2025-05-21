import { sortSectionsByFirstInPersonalSidebar } from '@app/homeV2/layout/LeftSidebar';
import { PersonaType } from '@app/homeV2/shared/types';

import { PersonalSidebarSection } from '@types';

type ReferenceSection = {
    id: string;
    component: React.ComponentType<any>;
    sectionName: PersonalSidebarSection;
    hideIfEmpty?: boolean;
    personas?: PersonaType[];
};

describe('sortSectionsByFirstInPersonalSidebar', () => {
    const mockSections: ReferenceSection[] = [
        {
            id: '1',
            component: () => null,
            sectionName: PersonalSidebarSection.YourAssets,
            personas: [PersonaType.BUSINESS_USER],
        },
        {
            id: '2',
            component: () => null,
            sectionName: PersonalSidebarSection.YourDomains,
            personas: [PersonaType.DATA_STEWARD],
        },
        {
            id: '3',
            component: () => null,
            sectionName: PersonalSidebarSection.YourGlossaryNodes,
            personas: [PersonaType.DATA_LEADER],
        },
    ];

    it('should put the specified section first', () => {
        const result = sortSectionsByFirstInPersonalSidebar(mockSections, PersonalSidebarSection.YourDomains);
        expect(result[0].sectionName).toBe(PersonalSidebarSection.YourDomains);
    });

    it('should maintain original order for other sections', () => {
        const result = sortSectionsByFirstInPersonalSidebar(mockSections, PersonalSidebarSection.YourDomains);
        expect(result[1].sectionName).toBe(PersonalSidebarSection.YourAssets);
        expect(result[2].sectionName).toBe(PersonalSidebarSection.YourGlossaryNodes);
    });

    it('should handle empty array', () => {
        const result = sortSectionsByFirstInPersonalSidebar([], PersonalSidebarSection.YourDomains);
        expect(result).toEqual([]);
    });

    it('should handle when firstInPersonalSidebar is not in the array', () => {
        const result = sortSectionsByFirstInPersonalSidebar(mockSections, PersonalSidebarSection.YourTags);
        expect(result).toEqual(mockSections);
    });

    it('should not mutate the input array', () => {
        const input = [...mockSections];
        sortSectionsByFirstInPersonalSidebar(input, PersonalSidebarSection.YourDomains);
        expect(input).toEqual(mockSections);
    });
});
