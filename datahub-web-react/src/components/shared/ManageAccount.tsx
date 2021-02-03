import React from 'react';
import { Avatar } from 'antd';
import { Link } from 'react-router-dom';
import defaultAvatar from '../../images/default_avatar.png';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';

interface Props {
    urn: string;
    pictureLink?: string;
}

const defaultProps = {
    pictureLink: undefined,
};

export const ManageAccount = ({ urn: _urn, pictureLink: _pictureLink }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Link to={`${entityRegistry.getPathName(EntityType.User)}/${_urn}`}>
            <Avatar
                style={{
                    marginRight: '15px',
                    color: '#f56a00',
                    backgroundColor: '#fde3cf',
                }}
                src={_pictureLink || defaultAvatar}
            />
        </Link>
    );
};

ManageAccount.defaultProps = defaultProps;
