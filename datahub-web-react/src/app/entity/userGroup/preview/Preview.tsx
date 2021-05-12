import React from 'react';
import { Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';

const NameText = styled(Typography.Title)`
    margin: 0;
    color: #0073b1;
`;
const TitleText = styled(Typography.Title)`
    color: rgba(0, 0, 0, 0.45);
`;

export const Preview = ({
    urn,
    name,
    title,
    photoUrl,
}: {
    urn: string;
    name: string;
    title?: string;
    photoUrl?: string;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <Link to={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${urn}`}>
            <Space size={28}>
                <CustomAvatar size={60} photoUrl={photoUrl} name={name} isGroup />
                <Space direction="vertical" size={4}>
                    <NameText level={3}>{name}</NameText>
                    <TitleText>{title}</TitleText>
                </Space>
            </Space>
        </Link>
    );
};
