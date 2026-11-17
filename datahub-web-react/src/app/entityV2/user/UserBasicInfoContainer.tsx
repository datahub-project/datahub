import { Icon, Tooltip } from '@components';
import { Phone } from '@phosphor-icons/react/dist/csr/Phone';
import { SlackLogo } from '@phosphor-icons/react/dist/csr/SlackLogo';
import { UserCircle } from '@phosphor-icons/react/dist/csr/UserCircle';
import { UsersThree } from '@phosphor-icons/react/dist/csr/UsersThree';
import { Divider } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
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
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

type Props = {
    name: string | undefined;
    dataHubRoleName: string;
    email: string | undefined;
    role: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
    team: string | undefined;
    manager: { urn: string; name: string } | undefined;
};

export const UserBasicInfoContainer = ({ name, dataHubRoleName, email, role, slack, phone, team, manager }: Props) => {
    const theme = useTheme();
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    // Teams are stored comma-joined (see UserProfile), the same format the edit modal splits on.
    const teamText = team
        ?.split(',')
        .map((part) => part.trim())
        .filter(Boolean)
        .join(', ');
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
                    {teamText && (
                        <SocialDetails>
                            <Icon icon={UsersThree} size="sm" color="inherit" />
                            <Tooltip title={t('user.teamTooltip', { team: teamText })}>
                                <span>{teamText}</span>
                            </Tooltip>
                        </SocialDetails>
                    )}
                    {manager && (
                        <SocialDetails>
                            <Icon icon={UserCircle} size="sm" color="inherit" />
                            <Tooltip title={t('user.managerTooltip', { manager: manager.name })}>
                                <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, manager.urn)}>
                                    {manager.name}
                                </Link>
                            </Tooltip>
                        </SocialDetails>
                    )}
                </SocialInfo>
            </BasicDetails>
        </BasicDetailsContainer>
    );
};
