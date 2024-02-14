// import { UserOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React from 'react';
import { Space, Badge, Typography, Divider } from 'antd';
import CustomAvatar from '../../shared/avatar/CustomAvatar';

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

const NameContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: left;
`;

export default function UserHeader({ profileSrc, name, title, skills, teams, email }: Props) {
    return (
        <Row>
            <div>
                <HeaderContainer>
                    <AvatarWrapper>
                        <CustomAvatar size={100} photoUrl={profileSrc || undefined} name={name || undefined} />
                    </AvatarWrapper>
                    <NameContainer>
                        <Typography.Title level={3}>{name}</Typography.Title>
                        <TitleContainer>
                            {title && (
                                <>
                                    <Typography.Text>{title}</Typography.Text>
                                    <Divider type="vertical" />
                                </>
                            )}
                            {email && (
                                <a href={`mailto:${email}`}>
                                    <Typography.Text strong>{email}</Typography.Text>
                                </a>
                            )}
                        </TitleContainer>
                    </NameContainer>
                </HeaderContainer>
                <div>
                    <Traits>
                        <Skills>
                            <Typography.Title level={5}>Ask me about</Typography.Title>
                            <Space>
                                {skills?.map((skill) => (
                                    <Badge style={{ backgroundColor: '#108ee9' }} count={skill} key={skill} />
                                ))}
                            </Space>
                        </Skills>
                        <div>
                            <Typography.Title level={5}>Teams</Typography.Title>
                            <Space>
                                {teams?.map((team) => (
                                    <Badge style={{ backgroundColor: '#87d068' }} count={team} key={team} />
                                ))}
                            </Space>
                        </div>
                    </Traits>
                </div>
            </div>
        </Row>
    );
}
