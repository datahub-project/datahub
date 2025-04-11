import React from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { AssetsYouOwn } from '../reference/sections/assets/AssetsYouOwn';
import { GroupsYouAreIn } from '../reference/sections/groups/GroupsYouAreIn';
import { TagsYouOwn } from '../reference/sections/tags/TagsYouOwn';
import { GlossaryNodesYouOwn } from '../reference/sections/glossary/GlossaryNodesYouOwn';
import { DomainsYouOwn } from '../reference/sections/domains/DomainsYouOwn';
import { ReferenceSectionProps } from '../reference/types';
import { PersonaType } from '../shared/types';
import { useUserPersona } from '../persona/useUserPersona';
import { UserHeader } from '../reference/header/UserHeader';
import { V2_HOME_PAGE_PERSONAL_SIDEBAR_ID } from '../../onboarding/configV2/HomePageOnboardingConfig';

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
];

// TODO: Make section ordering dynamic based on populated data.
export const LeftSidebar = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const currentUserPersona = useUserPersona();

    return (
        <Container id={V2_HOME_PAGE_PERSONAL_SIDEBAR_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                <UserHeader />
                <Body $isShowNavBarRedesign={isShowNavBarRedesign}>
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
