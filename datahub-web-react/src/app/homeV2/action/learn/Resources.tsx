import React from 'react';
import styled from 'styled-components';
import { ApiOutlined } from '@ant-design/icons';
import AutoStoriesOutlinedIcon from '@mui/icons-material/AutoStoriesOutlined';
import { BookOutlined, HelpCenterOutlined, OndemandVideoOutlined } from '@mui/icons-material';
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

const Icon = styled(BookOutlined)`
    margin-right: 8px;
    color: #9884d4;
    font-size: 16px;
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
    opacity: 0.9;
    transition: transform 0.3s ease, color 0.3s ease, opacity 0.3s ease;
    :hover {
        transform: scale(1.05);  // Slightly scale up the link on hover
        opacity: 1.0;
        color: #9884d4;
        text-decoration: underline;
    }
`;

const ResourceTitle = styled.div`
    margin-top: 8px;
    color: ${ANTD_GRAY[7]};
    text-align: center;
    opacity: 0.9;
    :hover {
        opacity: 1.0;
        color: #9884d4;
        text-decoration: underline;
    }
    `;

const ALL_GUIDES = [
    {
        id: 'integrations',
        title: 'Connect Sources',
        url: 'https://datahubproject.io/docs/ui-ingestion',
        icon: ApiOutlined,
        personas: [PersonaType.TECHNICAL_USER],
    },
    {
        id: 'features',
        title: 'Feature Guides',
        url: 'https://datahubproject.io/docs/category/features?utm_source=acryl_datahub_app',
        icon: HelpCenterOutlined,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'tutorials',
        title: 'How-To Tutorials',
        url: 'https://youtube.com/playlist?list=PLdCtLs64vZvErAXMiqUYH9e63wyDaMBgg&utm_source=acryl_datahub_app&utm_content=tutorials',
        icon: OndemandVideoOutlined,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
        ],
    },
    {
        id: 'case-studies',
        title: 'Case Studies',
        url: 'https://www.acryldata.io/customer-stories?utm_source=acryl_datahub_app&utm_content=case_studies',
        icon: OndemandVideoOutlined,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'blog',
        title: 'Subscribe to the Blog',
        url: 'https://www.acryldata.io/blog?utm_source=acryl_datahub_app&utm_content=blog',
        icon: AutoStoriesOutlinedIcon,
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
                    <Icon /> Resources
                </Title>
            </Header>
            <Section>
                {selectedGuides.map((guide) => (
                    <ResourceLink target="_blank" rel="noreferrer noopener" href={guide.url} key={guide.title}>
                        <guide.icon style={{ fontSize: 16, width: 18, height: 18 }} />
                        <ResourceTitle>{guide.title}</ResourceTitle>
                    </ResourceLink>
                ))}
            </Section>
        </Card>
    );
};
