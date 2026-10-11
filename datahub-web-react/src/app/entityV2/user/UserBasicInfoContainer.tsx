import { Icon, Tooltip } from '@components';
import { Phone } from '@phosphor-icons/react/dist/csr/Phone';
import { SlackLogo } from '@phosphor-icons/react/dist/csr/SlackLogo';
import { Divider } from 'antd';
import React from 'react';
import { useTheme } from 'styled-components';

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

type Props = {
    name: string | undefined;
    dataHubRoleName: string;
    email: string | undefined;
    role: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
};

export const UserBasicInfoContainer = ({ name, dataHubRoleName, email, role, slack, phone }: Props) => {
    const theme = useTheme();
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
                    <TitleRole>{role || <EmptyValue color={theme.colors.bg} />}</TitleRole>
                </NameTitleContainer>
                <Divider />
                <SocialInfo>
                    <SocialDetails>
                        <DraftsOutlinedIconStyle />
                        <Tooltip title={email}>{email || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                    <SocialDetails>
                        <Icon icon={SlackLogo} size="sm" color="inherit" />
                        <Tooltip title={slack}>{slack || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                    <SocialDetails>
                        <Icon icon={Phone} size="sm" color="inherit" />
                        <Tooltip title={phone}>{phone || <EmptyValue />}</Tooltip>
                    </SocialDetails>
                </SocialInfo>
            </BasicDetails>
        </BasicDetailsContainer>
    );
};
