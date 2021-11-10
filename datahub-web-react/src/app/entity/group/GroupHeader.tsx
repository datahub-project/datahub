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

export default function GroupHeader({ name, description, email }: Props) {
    // TODO: Add Optional Group Image URLs
    return (
        <>
            <Row>
                <AvatarWrapper>
                    <CustomAvatar size={100} photoUrl={undefined} name={name || undefined} />
                </AvatarWrapper>
                <div>
                    <Typography.Title level={3} style={{ marginTop: 8 }}>
                        {name}
                    </Typography.Title>
                    <Space split="|" size="middle">
                        <a href={`mailto:${email}`}>
                            <Typography.Text strong>{email}</Typography.Text>
                        </a>
                    </Space>
                </div>
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
