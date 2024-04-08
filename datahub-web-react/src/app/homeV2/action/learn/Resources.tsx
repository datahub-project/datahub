import React from 'react';
import styled from 'styled-components';
import { ThunderboltOutlined, QuestionCircleOutlined, ApiOutlined, HeartOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { PersonaType } from '../../shared/types';
import { useUserPersona } from '../../persona/useUserPersona';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    margin: 8px 0px 20px 0px;
`;

const Title = styled.div`
    font-weight: 600;
    font-size: 14px;
    color: #434863;
    word-break: break-word;
    display: flex;
    align-items: center;
`;

const Icon = styled(ThunderboltOutlined)`
    margin-right: 8px;
    color: #3cb47a;
    font-size: 18px;
`;

const Section = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-evenly;
    padding: 12px 0px;
`;

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 12px 20px 20px 20px;
`;

const ResourceLink = styled.a`
    color: ${ANTD_GRAY[8]};
    padding: 0px 16px;
    font-size: 14px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    :hover {
        opacity: 0.8;
    }
`;

const ResourceTitle = styled.div`
    margin-top: 8px;
    color: ${ANTD_GRAY[7]};
    text-align: center;
`;

const ALL_GUIDES = [
    {
        id: 'integrations',
        title: 'Connect Data',
        url: 'https://datahubproject.io/docs/metadata-ingestion',
        icon: ApiOutlined,
        personas: [PersonaType.TECHNICAL_USER],
    },
    {
        id: 'features',
        title: 'Feature Guides',
        url: 'https://datahubproject.io/docs/ui-ingestion',
        icon: QuestionCircleOutlined,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'community',
        title: 'Join the Community',
        url: 'https://slack.datahubproject.io/',
        icon: HeartOutlined,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
];

export const Resources = () => {
    const currentUserPersona = useUserPersona();
    const selectedGuides = ALL_GUIDES.filter(
        (guide) => !guide.personas || guide.personas.includes(currentUserPersona),
    ).slice(0, 3);
    return (
        <Card>
            <Header>
                <Title>
                    <Icon /> Quick Links
                </Title>
            </Header>
            <Section>
                {selectedGuides.map((guide) => (
                    <ResourceLink target="_blank" rel="noreferrer noopener" href={guide.url} key={guide.title}>
                        <guide.icon style={{ fontSize: 20, width: 18, height: 18, color: ANTD_GRAY[8] }} />
                        <ResourceTitle>{guide.title}</ResourceTitle>
                    </ResourceLink>
                ))}
            </Section>
        </Card>
    );
};
