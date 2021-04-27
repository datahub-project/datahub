import { Avatar, Tooltip } from 'antd';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import defaultAvatar from '../../../images/default_avatar.png';

const AvatarStyled = styled(Avatar)<{ size?: number }>`
    color: #fff;
    background-color: #ccc;
    text-align: center;
    font-size: ${(props) => (props.size ? `${Math.max(props.size / 2.0, 14)}px` : '14px')} !important;
    && > span {
        transform: scale(1) translateX(-46%) translateY(-3%) !important;
    }
`;

type Props = {
    url?: string;
    photoUrl?: string;
    useDefaultAvatar?: boolean;
    name?: string;
    style?: React.CSSProperties;
    placement?: TooltipPlacement;
    size?: number;
};

export default function CustomAvatar({ url, photoUrl, useDefaultAvatar, name, style, placement, size }: Props) {
    const avatarWithInitial = name ? (
        <AvatarStyled style={style} size={size}>
            {name.charAt(0).toUpperCase()}
        </AvatarStyled>
    ) : (
        <AvatarStyled src={defaultAvatar} style={style} size={size} />
    );
    const avatarWithDefault = useDefaultAvatar ? (
        <AvatarStyled src={defaultAvatar} style={style} size={size} />
    ) : (
        avatarWithInitial
    );
    const avatar = photoUrl ? <AvatarStyled src={photoUrl} style={style} size={size} /> : avatarWithDefault;
    if (!name) {
        return url ? <Link to={url}>{avatar}</Link> : avatar;
    }
    return (
        <Tooltip title={name} placement={placement}>
            {url ? <Link to={url}>{avatar}</Link> : avatar}
        </Tooltip>
    );
}
