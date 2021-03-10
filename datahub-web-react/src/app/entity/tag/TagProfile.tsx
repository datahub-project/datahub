import { grey } from '@ant-design/colors';
import { Alert, Avatar, Card, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { useParams } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useGetTagQuery } from '../../../graphql/tag.generated';
import defaultAvatar from '../../../images/default_avatar.png';
import { EntityType } from '../../../types.generated';
import { Message } from '../../shared/Message';
import { useEntityRegistry } from '../../useEntityRegistry';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

const TitleLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 13;
    }
`;

const TitleText = styled(Typography.Title)`
    &&& {
        margin-top: 0px;
    }
`;

type TagPageParams = {
    urn: string;
};

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagProfile() {
    const { urn } = useParams<TagPageParams>();
    const { loading, error, data } = useGetTagQuery({ variables: { urn } });
    const entityRegistry = useEntityRegistry();

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <Card
                title={
                    <>
                        <Space direction="vertical" size="middle">
                            <div>
                                <TitleLabel>Tag</TitleLabel>
                                <TitleText>{data?.tag?.name}</TitleText>
                            </div>
                            <Avatar.Group maxCount={6} size="large">
                                {data?.tag?.ownership?.owners?.map((owner) => (
                                    <Tooltip title={owner.owner.info?.fullName} key={owner.owner.urn}>
                                        <Link
                                            to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${
                                                owner.owner.urn
                                            }`}
                                        >
                                            <Avatar
                                                src={owner.owner?.editableInfo?.pictureLink || defaultAvatar}
                                                data-testid={`avatar-tag-${owner.owner.urn}`}
                                            />
                                        </Link>
                                    </Tooltip>
                                ))}
                            </Avatar.Group>
                        </Space>
                    </>
                }
            >
                <Typography.Paragraph strong style={{ color: grey[2], fontSize: 13 }}>
                    Description
                </Typography.Paragraph>
                <Typography.Text>{data?.tag?.description}</Typography.Text>
            </Card>
        </PageContainer>
    );
}
