import { Avatar } from 'antd';
import { Tooltip } from '@components';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import defaultAvatar from '../../../images/default_avatar.png';
import getAvatarColor from '../../shared/avatar/getAvatarColor';

const AvatarStyled = styled(Avatar)<{ size?: number; $backgroundColor?: string }>`
    color: #fff;
    background-color: ${(props) => (props.$backgroundColor ? `${props.$backgroundColor}` : 'transparent')};
    font-size: ${(props) => (props.size ? `${Math.max(props.size / 2.0, 10)}px` : '10px')} !important;
    height: ${(props) => (props.size ? props.size : 20)}px;
    width: ${(props) => (props.size ? props.size : 20)}px;

    .ant-avatar-string {
        text-align: center;
        top: 0px;
        line-height: ${(props) => (props.size ? props.size : 20)}px;
    }
    :hover {
        cursor: pointer;
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
    isPolicy?: boolean;
    isRole?: boolean;
    hideTooltip?: boolean;
};

export default function ActorAvatar({
    url,
    photoUrl,
    useDefaultAvatar,
    name,
    style,
    placement,
    size,
    isGroup = false,
    isPolicy = false,
    isRole = false,
    hideTooltip = false,
}: Props) {
    const history = useHistory();
    const isEmbeddedProfile = useIsEmbeddedProfile();

    const navigate = () => {
        if (url) {
            if (isEmbeddedProfile) window.open(url, '_blank');
            else history.push(url);
        }
    };

    const avatarWithInitial = name ? (
        <AvatarStyled onClick={navigate} style={style} size={size} $backgroundColor={getAvatarColor(name)}>
            {name.charAt(0).toUpperCase()}
        </AvatarStyled>
    ) : (
        <AvatarStyled src={defaultAvatar} style={style} size={size} $backgroundColor={getAvatarColor(name)} />
    );
    const avatarWithDefault = useDefaultAvatar ? (
        <AvatarStyled
            onClick={navigate}
            src={defaultAvatar}
            style={style}
            size={size}
            $backgroundColor={getAvatarColor(name)}
        />
    ) : (
        avatarWithInitial
    );
    const avatar =
        photoUrl && photoUrl !== '' ? (
            <AvatarStyled onClick={navigate} src={photoUrl} style={style} size={size} />
        ) : (
            avatarWithDefault
        );
    if (!name) {
        return avatar;
    }

    const renderTitle = (input) => {
        let title = `${input}`;
        if (isGroup) {
            title = `${title} - Group`;
        } else if (isPolicy) {
            title = `${title}`;
        } else if (isRole) {
            title = `${title} - Role`;
        }
        return title;
    };

    return hideTooltip ? (
        avatar
    ) : (
        <Tooltip title={renderTitle(name)} placement={placement} showArrow={false}>
            {avatar}
        </Tooltip>
    );
}
