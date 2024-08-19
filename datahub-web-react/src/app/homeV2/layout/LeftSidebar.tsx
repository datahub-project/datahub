import React from 'react';
import styled from 'styled-components';
import { AssetsYouOwn } from '../reference/sections/assets/AssetsYouOwn';
import { AssetsYouSubscribeTo } from '../reference/sections/subscriptions/AssetsYouSubscribeTo';
import { GroupsYouAreIn } from '../reference/sections/groups/GroupsYouAreIn';
import { TagsYouOwn } from '../reference/sections/tags/TagsYouOwn';
import { GlossaryNodesYouOwn } from '../reference/sections/glossary/GlossaryNodesYouOwn';
import { DomainsYouOwn } from '../reference/sections/domains/DomainsYouOwn';
import { ReferenceSectionProps } from '../reference/types';
import { PersonaType } from '../shared/types';
import { useUserPersona } from '../persona/useUserPersona';
import { UserHeader } from '../reference/header/UserHeader';
import { PinnedLinks } from '../reference/sections/pinned/PinnedLinks';
import { V2_HOME_PAGE_PERSONAL_SIDEBAR_ID } from '../../onboarding/configV2/HomePageOnboardingConfig';

const Container = styled.div`
    flex: 1;
    max-width: 380px;
    overflow-y: auto;
    padding: 0px 12px 12px 0px;
    height: calc(100vh - 72px);
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const Content = styled.div`
    background-color: #ffffff;
    border-radius: 18px;
    border: 1.5px solid #efefef;
    min-height: 100%;
`;

const Body = styled.div`
    padding: 12px 20px 0px 20px;
`;

type ReferenceSection = {
    id: string;
    component: React.ComponentType<ReferenceSectionProps>;
    hideIfEmpty?: boolean;
    personas?: PersonaType[];
};

const ALL_SECTIONS: ReferenceSection[] = [
    {
        id: 'AssetsYouOwn',
        component: AssetsYouOwn,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.DATA_ENGINEER,
        ],
    },
    {
        id: 'AssetsYouSubscribeTo',
        component: AssetsYouSubscribeTo,
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
        hideIfEmpty: true,
        personas: [PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'TagsYouOwn',
        component: TagsYouOwn,
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
        hideIfEmpty: true,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
        ],
    },
    {
        id: 'PinnedLinks',
        component: PinnedLinks,
        hideIfEmpty: true,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.DATA_ENGINEER,
        ],
    },
];

// TODO: Make section ordering dynamic based on populated data.
export const LeftSidebar = () => {
    const currentUserPersona = useUserPersona();
    return (
        <Container id={V2_HOME_PAGE_PERSONAL_SIDEBAR_ID}>
            <Content>
                <UserHeader />
                <Body>
                    {ALL_SECTIONS.filter(
                        (section) => !section.personas || section.personas.includes(currentUserPersona),
                    ).map((section) => (
                        <section.component key={section.id} hideIfEmpty={section.hideIfEmpty} />
                    ))}
                </Body>
            </Content>
        </Container>
    );
};
