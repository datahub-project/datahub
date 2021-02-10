import { UserOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React from 'react';
import { Space, Badge, Typography, Avatar } from 'antd';

type Props = {
    profileSrc?: string | null;
    name?: string | null;
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

export default function UserHeader({ profileSrc, name, title, skills, teams, email }: Props) {
    return (
        <Row>
            <AvatarWrapper>
                <Avatar icon={<UserOutlined />} src={profileSrc} size={100} />
            </AvatarWrapper>
            <div>
                <Typography.Title level={3}>{name}</Typography.Title>
                <Space split="|" size="middle">
                    <Typography.Text>{title}</Typography.Text>
                    <a href={`mailto:${email}`}>
                        <Typography.Text strong>{email}</Typography.Text>
                    </a>
                </Space>
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
