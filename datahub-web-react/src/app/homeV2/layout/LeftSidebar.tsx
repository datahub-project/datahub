import React from 'react';
import styled from 'styled-components';

import { useUserPersona } from '@app/homeV2/persona/useUserPersona';
import { UserHeader } from '@app/homeV2/reference/header/UserHeader';
import { AssetsYouOwn } from '@app/homeV2/reference/sections/assets/AssetsYouOwn';
import { DomainsYouOwn } from '@app/homeV2/reference/sections/domains/DomainsYouOwn';
import { GlossaryNodesYouOwn } from '@app/homeV2/reference/sections/glossary/GlossaryNodesYouOwn';
import { GroupsYouAreIn } from '@app/homeV2/reference/sections/groups/GroupsYouAreIn';
import { TagsYouOwn } from '@app/homeV2/reference/sections/tags/TagsYouOwn';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { PersonaType } from '@app/homeV2/shared/types';
import { V2_HOME_PAGE_PERSONAL_SIDEBAR_ID } from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { PersonalSidebarSection } from '@types';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    max-width: 380px;
    overflow-y: auto;
    ${(props) => !props.$isShowNavBarRedesign && 'padding: 0px 12px 12px 0px;'}
    height: ${(props) => (props.$isShowNavBarRedesign ? 'calc(100vh - 88px)' : 'calc(100vh - 72px)')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: 5px;
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}

    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '18px'};
    min-height: 100%;
    ${(props) => !props.$isShowNavBarRedesign && 'border: 1.5px solid #efefef;'}
`;

const Body = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding: ${(props) => (props.$isShowNavBarRedesign ? '16px 20px' : '12px 20px 0px 20px')};
`;

type ReferenceSection = {
    id: string;
    component: React.ComponentType<ReferenceSectionProps>;
    sectionName: PersonalSidebarSection;
    hideIfEmpty?: boolean;
    personas?: PersonaType[];
};

const ALL_SECTIONS: ReferenceSection[] = [
    {
        id: 'AssetsYouOwn',
        component: AssetsYouOwn,
        sectionName: PersonalSidebarSection.YourAssets,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.DATA_ENGINEER,
        ],
    },
    {
        id: 'DomainsYouOwn',
        component: DomainsYouOwn,
        sectionName: PersonalSidebarSection.YourDomains,
        hideIfEmpty: true,
        personas: [
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
        ],
    },
    {
        id: 'GlossaryNodesYouOwn',
        component: GlossaryNodesYouOwn,
        sectionName: PersonalSidebarSection.YourGlossaryNodes,
        hideIfEmpty: true,
        personas: [PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'TagsYouOwn',
        component: TagsYouOwn,
        sectionName: PersonalSidebarSection.YourTags,
        hideIfEmpty: true,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.DATA_ENGINEER,
        ],
    },
    {
        id: 'GroupsYouAreIn',
        component: GroupsYouAreIn,
        sectionName: PersonalSidebarSection.YourGroups,
        hideIfEmpty: true,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
        ],
    },
];

/**
 * Sorts sections so that the section matching firstInPersonalSidebar appears first,
 * while maintaining the original order for all other sections.
 */
export const sortSectionsByFirstInPersonalSidebar = (
    sections: ReferenceSection[],
    firstInPersonalSidebar: PersonalSidebarSection,
): ReferenceSection[] => {
    return [...sections].sort((a, b) => {
        if (a.sectionName === firstInPersonalSidebar) return -1;
        if (b.sectionName === firstInPersonalSidebar) return 1;
        return 0;
    });
};

export const LeftSidebar = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const currentUserPersona = useUserPersona();
    const { config } = useAppConfig();
    const { firstInPersonalSidebar } = config.homePageConfig;
    const finalSections = sortSectionsByFirstInPersonalSidebar(
        ALL_SECTIONS.filter((section) => !section.personas || section.personas.includes(currentUserPersona)),
        firstInPersonalSidebar,
    );

    return (
        <Container id={V2_HOME_PAGE_PERSONAL_SIDEBAR_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                <UserHeader />
                <Body $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {finalSections.map((section) => (
                        <section.component key={section.id} hideIfEmpty={section.hideIfEmpty} />
                    ))}
                </Body>
            </Content>
        </Container>
    );
};
