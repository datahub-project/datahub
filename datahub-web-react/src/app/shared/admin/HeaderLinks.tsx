import styled from 'styled-components/macro';
import * as React from 'react';
import {Trans} from "react-i18next";
import {t} from "i18next";
import {
    ApiOutlined,
    BarChartOutlined,
    BookOutlined,
    SettingOutlined,
    FolderOutlined,
    SolutionOutlined,
    DownOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button, Dropdown, Menu, Tooltip } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepIdsAllowlist } from '../../onboarding/useUpdateEducationStepIdsAllowlist';
import { useUserContext } from '../../context/useUserContext';
import {HeaderTranslate} from "../../../components/shared/HeaderTranslate"

const LinkWrapper = styled.span`
    margin-right: 0px;
`;

const LinksWrapper = styled.div<{ areLinksHidden?: boolean }>`
    opacity: 1;
    white-space: nowrap;
    transition: opacity 0.5s;

    ${(props) =>
    props.areLinksHidden &&
    `
        opacity: 0;
        width: 0;
    `}
`;

const MenuItem = styled(Menu.Item)`
    font-size: 12px;
    font-weight: bold;
    max-width: 240px;
`;

const NavTitleContainer = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
    padding: 2px;
`;

const NavTitleText = styled.span`
    margin-left: 6px;
`;

const NavTitleDescription = styled.div`
    font-size: 12px;
    font-weight: normal;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    areLinksHidden?: boolean;
}

export function HeaderLinks(props: Props) {
    const { areLinksHidden } = props;
    const me = useUserContext();
    const { config } = useAppConfig();

    const isAnalyticsEnabled = config?.analyticsConfig.enabled;
    const isIngestionEnabled = config?.managedIngestionConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges?.manageIngestion && me.platformPrivileges?.manageSecrets;
    const showDomains = me?.platformPrivileges?.createDomains || me?.platformPrivileges?.manageDomains;

    useUpdateEducationStepIdsAllowlist(!!showIngestion, HOME_PAGE_INGESTION_ID);

    return (
        <LinksWrapper areLinksHidden={areLinksHidden}>
            {showAnalytics && (
                <LinkWrapper>
                    <Link to="/analytics">
                        <Button type="text">
                            <Tooltip title={t ("View DataHub usage analytics")}>
                                <NavTitleContainer>
                                    <BarChartOutlined />
                                    <NavTitleText><Trans>Analytics</Trans></NavTitleText>
                                </NavTitleContainer>
                            </Tooltip>
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
            {showIngestion && (
                <LinkWrapper>
                    <Link to="/ingestion">
                        <Button id={HOME_PAGE_INGESTION_ID} type="text">
                            <Tooltip title={t ("Connect DataHub to your organization's data sources")}>
                                <NavTitleContainer>
                                    <ApiOutlined />
                                    <NavTitleText><Trans>Ingestion</Trans></NavTitleText>
                                </NavTitleContainer>
                            </Tooltip>
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
            <Dropdown
                trigger={['click']}
                overlay={
                    <Menu>
                        <MenuItem key="0">
                            <Link to="/glossary">
                                <NavTitleContainer>
                                    <BookOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                    <NavTitleText><Trans>Glossary</Trans></NavTitleText>
                                </NavTitleContainer>
                                <NavTitleDescription><Trans>View and modify your data dictionary</Trans></NavTitleDescription>
                            </Link>
                        </MenuItem>
                        {showDomains && (
                            <MenuItem key="1">
                                <Link to="/domains">
                                    <NavTitleContainer>
                                        <FolderOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                        <NavTitleText><Trans>Domains</Trans></NavTitleText>
                                    </NavTitleContainer>
                                    <NavTitleDescription><Trans>Manage related groups of data assets</Trans></NavTitleDescription>
                                </Link>
                            </MenuItem>
                        )}
                    </Menu>
                }
            >
                <LinkWrapper>
                    <Button type="text">
                        <SolutionOutlined /> <Trans>Govern</Trans> <DownOutlined style={{ fontSize: '6px' }} />
                    </Button>
                </LinkWrapper>
            </Dropdown>
            <HeaderTranslate/>
            {showSettings && (
                <LinkWrapper style={{ marginRight: 12 }}>
                    <Link to="/settings">
                        <Button type="text">
                            <Tooltip title={t ("Manage your DataHub settings")}>
                                <SettingOutlined />
                            </Tooltip>
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
        </LinksWrapper>
    );
}