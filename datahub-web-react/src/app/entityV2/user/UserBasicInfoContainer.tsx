import { PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Divider } from 'antd';
import React from 'react';

import {
    BasicDetails,
    BasicDetailsContainer,
    DraftsOutlinedIconStyle,
    EmptyValue,
    Name,
    NameTitleContainer,
    RoleName,
    SocialDetails,
    SocialInfo,
    TitleRole,
} from '@app/entityV2/shared/SidebarStyledComponents';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

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
