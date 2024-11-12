import React from 'react';
import { Divider } from 'antd';
import { Tooltip } from '@components';
import { PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import {
    EmptyValue,
    SocialDetails,
    Name,
    TitleRole,
    RoleName,
    BasicDetailsContainer,
    DraftsOutlinedIconStyle,
    BasicDetails,
    NameTitleContainer,
    SocialInfo,
} from '../shared/SidebarStyledComponents';
import { REDESIGN_COLORS } from '../shared/constants';

type Props = {
    name: string | undefined;
    dataHubRoleName: string;
    email: string | undefined;
    role: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
};

export const UserBasicInfoContainer = ({ name, dataHubRoleName, email, role, slack, phone }: Props) => {
    return (
        <BasicDetailsContainer>
            <BasicDetails>
                <NameTitleContainer>
                    <Name>
                        <Tooltip title={name}>
                            <span>{name || <EmptyValue />}</span>
                        </Tooltip>
                        {dataHubRoleName ? <RoleName>{dataHubRoleName}</RoleName> : null}
                    </Name>
                    <TitleRole>{role || <EmptyValue color={REDESIGN_COLORS.WHITE} />}</TitleRole>
                </NameTitleContainer>
                <Divider />
                <SocialInfo>
                    <SocialDetails>
                        <DraftsOutlinedIconStyle />
                        <Tooltip title={email}>{email || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                    <SocialDetails>
                        <SlackOutlined />
                        <Tooltip title={slack}>{slack || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                    <SocialDetails>
                        <PhoneOutlined />
                        <Tooltip title={phone}>{phone || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                </SocialInfo>
            </BasicDetails>
        </BasicDetailsContainer>
    );
};
