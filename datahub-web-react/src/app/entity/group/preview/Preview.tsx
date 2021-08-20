import React from 'react';
import { Avatar, Row, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { CorpUser, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { CustomAvatar } from '../../../shared/avatar';

const NameText = styled(Typography.Title)`
    margin: 0;
    color: #0073b1;
`;
const DescriptionText = styled(Typography.Paragraph)`
    color: rgba(0, 0, 0, 1);
`;

export const Preview = ({
    urn,
    name,
    description,
    members,
}: {
    urn: string;
    name: string;
    description?: string | null;
    members?: Array<CorpUser> | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, urn)} style={{ width: '100%' }}>
            <Row justify="space-between">
                <Space direction="vertical" size={4}>
                    <NameText level={3}>{name}</NameText>
                    {description?.length === 0 ? (
                        <DescriptionText type="secondary">No description</DescriptionText>
                    ) : (
                        <DescriptionText>{description}</DescriptionText>
                    )}
                </Space>
                <Avatar.Group maxCount={3} size="default" style={{ marginTop: 12 }}>
                    {(members || [])?.map((member, key) => (
                        // eslint-disable-next-line react/no-array-index-key
                        <div data-testid={`avatar-tag-${member.urn}`} key={`${member.urn}-${key}`}>
                            <CustomAvatar
                                name={
                                    member.info?.fullName ||
                                    member.info?.displayName ||
                                    member.info?.firstName ||
                                    member.info?.email ||
                                    member.username
                                }
                                url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${member.urn}`}
                                photoUrl={member.editableInfo?.pictureLink || undefined}
                            />
                        </div>
                    ))}
                </Avatar.Group>
            </Row>
        </Link>
    );
};
