import { Avatar, Tooltip } from 'antd';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import defaultAvatar from '../../../images/default_avatar.png';

const AvatarStyled = styled(Avatar)`
    color: #fff;
    background-color: #ccc;
    font-size: 18px;
`;

type Props = {
    url?: string;
    photoUrl?: string;
    useDefaultAvatar?: boolean;
    name?: string;
    style?: React.CSSProperties;
    placement?: TooltipPlacement;
};

export default function CustomAvatar({ url, photoUrl, useDefaultAvatar, name, style, placement }: Props) {
    const avatarWithInitial = name ? (
        <AvatarStyled style={style}>{name.charAt(0).toUpperCase()}</AvatarStyled>
    ) : (
        <AvatarStyled src={defaultAvatar} style={style} />
    );
    const avatarWithDefault = useDefaultAvatar ? <AvatarStyled src={defaultAvatar} style={style} /> : avatarWithInitial;
    return (
        <Tooltip title={name} placement={placement}>
            <Link to={url}>{photoUrl ? <AvatarStyled src={photoUrl} style={style} /> : avatarWithDefault}</Link>
        </Tooltip>
    );
}
