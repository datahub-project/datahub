import { Avatar, Tooltip } from 'antd';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import defaultAvatar from '../../../images/default_avatar.png';

const AvatarStyled = styled(Avatar)<{ size?: number; isgroup?: string }>`
    color: #fff;
    background-color: ${(props) =>
        props.isgroup ? '#ccc' : '#ccc'}; // TODO: make it different style for corpGroup vs corpUser
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
    isGroup?: boolean;
};

export default function CustomAvatar({
    url,
    photoUrl,
    useDefaultAvatar,
    name,
    style,
    placement,
    size,
    isGroup = false,
}: Props) {
    const avatarWithInitial = name ? (
        <AvatarStyled style={style} size={size} isgroup={isGroup ? 'true' : undefined}>
            {name.charAt(0).toUpperCase()}
        </AvatarStyled>
    ) : (
        <AvatarStyled src={defaultAvatar} style={style} size={size} isgroup={isGroup ? 'true' : undefined} />
    );
    const avatarWithDefault = useDefaultAvatar ? (
        <AvatarStyled src={defaultAvatar} style={style} size={size} isgroup={isGroup ? 'true' : undefined} />
    ) : (
        avatarWithInitial
    );
    const avatar =
        photoUrl && photoUrl !== '' ? (
            <AvatarStyled src={photoUrl} style={style} size={size} isgroup={isGroup ? 'true' : undefined} />
        ) : (
            avatarWithDefault
        );
    if (!name) {
        return url ? <Link to={url}>{avatar}</Link> : avatar;
    }
    return (
        <Tooltip title={isGroup ? `${name} - Group` : name} placement={placement}>
            {url ? <Link to={url}>{avatar}</Link> : avatar}
        </Tooltip>
    );
}
