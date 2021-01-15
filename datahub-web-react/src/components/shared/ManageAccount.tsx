import { Avatar } from 'antd';
import * as React from 'react';
import { Link } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import defaultAvatar from '../../images/default_avatar.png';

interface Props {
    urn: string;
    pictureLink?: string;
}

const defaultProps = {
    pictureLink: undefined,
};

export const ManageAccount = ({ urn: _urn, pictureLink: _pictureLink }: Props) => {
    return (
        <Link to={`${PageRoutes.USERS}/${_urn}`}>
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
