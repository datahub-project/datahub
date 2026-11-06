import { Avatar, Divider, Heading, Pill, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

type Props = {
    profileSrc?: string | null;
    name: string;
    title?: string | null;
    skills?: string[] | null;
    teams?: string[] | null;
    email?: string | null;
};

const Row = styled.div`
    display: inline-flex;
`;

const AvatarWrapper = styled.div`
    margin-right: 32px;
`;

const Traits = styled.div`
    display: inline-flex;
    margin-top: 32px;
`;

const Skills = styled.div`
    margin-right: 32px;
`;

const TitleContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

// Keep the bottom margins antd's Title levels 3 and 5 had.
const NameHeadingWrapper = styled.div`
    margin-bottom: 12px;
`;

const SectionHeadingWrapper = styled.div`
    margin-bottom: 8px;
`;

const PillList = styled.div`
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
`;

const NameContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: left;
`;

export default function UserHeader({ profileSrc, name, title, skills, teams, email }: Props) {
    const { t } = useTranslation('entity.types');

    return (
        <Row>
            <div>
                <HeaderContainer>
                    <AvatarWrapper>
                        <Avatar
                            name={name || ''}
                            imageUrl={profileSrc || undefined}
                            type={AvatarType.user}
                            size="3xl"
                        />
                    </AvatarWrapper>
                    <NameContainer>
                        <NameHeadingWrapper>
                            <Heading type="h3" size="4xl" weight="semiBold">
                                {name}
                            </Heading>
                        </NameHeadingWrapper>
                        <TitleContainer>
                            {title && (
                                <>
                                    <Text type="span" size="inherit" style={{ lineHeight: 'inherit' }}>
                                        {title}
                                    </Text>
                                    <Divider type="vertical" />
                                </>
                            )}
                            {email && (
                                <a href={`mailto:${email}`}>
                                    <Text
                                        type="span"
                                        size="inherit"
                                        style={{ lineHeight: 'inherit' }}
                                        weight="semiBold"
                                    >
                                        {email}
                                    </Text>
                                </a>
                            )}
                        </TitleContainer>
                    </NameContainer>
                </HeaderContainer>
                <div>
                    <Traits>
                        <Skills>
                            <SectionHeadingWrapper>
                                <Heading type="h5" size="lg" weight="semiBold">
                                    {t('user.askMeAbout')}
                                </Heading>
                            </SectionHeadingWrapper>
                            <PillList>
                                {skills?.map((skill) => (
                                    <Pill label={skill} key={skill} color="primary" clickable={false} />
                                ))}
                            </PillList>
                        </Skills>
                        <div>
                            <SectionHeadingWrapper>
                                <Heading type="h5" size="lg" weight="semiBold">
                                    {t('user.teamsHeading')}
                                </Heading>
                            </SectionHeadingWrapper>
                            <PillList>
                                {teams?.map((team) => <Pill label={team} key={team} color="green" clickable={false} />)}
                            </PillList>
                        </div>
                    </Traits>
                </div>
            </div>
        </Row>
    );
}
