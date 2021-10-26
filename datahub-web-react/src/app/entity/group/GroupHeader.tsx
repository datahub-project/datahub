import styled from 'styled-components';
import React from 'react';
import { Space, Typography } from 'antd';
import CustomAvatar from '../../shared/avatar/CustomAvatar';

type Props = {
    name?: string | null;
    email?: string | null;
    description?: string | null;
};

const Row = styled.div`
    display: inline-flex;
`;

const AvatarWrapper = styled.div`
    margin-right: 32px;
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

const TitleContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

export default function GroupHeader({ name, description, email }: Props) {
    // TODO: Add Optional Group Image URLs
    return (
        <>
            <Row>
                <HeaderContainer>
                    <AvatarWrapper>
                        <CustomAvatar size={100} photoUrl={undefined} name={name || undefined} />
                    </AvatarWrapper>
                    <NameContainer>
                        <Typography.Title level={3} style={{ marginTop: 8 }}>
                            {name}
                        </Typography.Title>
                        <TitleContainer>
                            {email && (
                                <a href={`mailto:${email}`}>
                                    <Typography.Text strong>{email}</Typography.Text>
                                </a>
                            )}
                        </TitleContainer>
                    </NameContainer>
                </HeaderContainer>
            </Row>
            <Typography.Title style={{ marginTop: 40 }} level={5}>
                Description
            </Typography.Title>
            <Space>
                <Typography.Paragraph>{description}</Typography.Paragraph>
            </Space>
        </>
    );
}
