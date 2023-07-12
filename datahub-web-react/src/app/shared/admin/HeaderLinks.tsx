import styled from 'styled-components/macro';
import * as React from 'react';
import {
    ApiOutlined,
    BarChartOutlined,
    InboxOutlined,
    BookOutlined,
    SettingOutlined,
    FolderOutlined,
    FileDoneOutlined,
    SolutionOutlined,
    DownOutlined,
    EyeOutlined,
    DatabaseOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button, Dropdown, Menu, Tooltip } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepIdsAllowlist } from '../../onboarding/useUpdateEducationStepIdsAllowlist';
import { useUserContext } from '../../context/useUserContext';
import { PageRoutes } from '../../../conf/Global';

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
    max-width: 400px;
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

const StyledDatabaseOutlined = styled(DatabaseOutlined)`
    && {
        font-size: 14px;
        font-weight: bold;
    }
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
    // SaaS Only
    // Currently we only have a flag for metadata proposals.
    // In the future, we may add configs for alerts, announcements, etc.
    const isActionRequestsEnabled = config?.actionRequestsConfig.enabled;
    const isTestsEnabled = config?.testsConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges?.manageIngestion && me.platformPrivileges?.manageSecrets;
    const showDomains = me?.platformPrivileges?.createDomains || me?.platformPrivileges?.manageDomains;

    // SaaS only
    const showActionRequests = (isActionRequestsEnabled && me?.platformPrivileges?.viewMetadataProposals) || false;
    const showTests = (isTestsEnabled && me?.platformPrivileges?.manageTests) || false;
    const showDatasetHealth = config?.featureFlags?.datasetHealthDashboardEnabled;
    const showObserve = showDatasetHealth;

    useUpdateEducationStepIdsAllowlist(!!showIngestion, HOME_PAGE_INGESTION_ID);

    return (
        <LinksWrapper areLinksHidden={areLinksHidden}>
            {showAnalytics && (
                <LinkWrapper>
                    <Link to="/analytics">
                        <Button type="text">
                            <Tooltip title="View DataHub usage analytics">
                                <NavTitleContainer>
                                    <BarChartOutlined />
                                    <NavTitleText>Analytics</NavTitleText>
                                </NavTitleContainer>
                            </Tooltip>
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
            {showActionRequests && (
                <LinkWrapper>
                    <Link to="/requests">
                        <Button type="text">
                            <InboxOutlined /> Inbox
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
            {showIngestion && (
                <LinkWrapper>
                    <Link to="/ingestion">
                        <Button id={HOME_PAGE_INGESTION_ID} type="text">
                            <Tooltip title="Connect DataHub to your organization's data sources">
                                <NavTitleContainer>
                                    <ApiOutlined />
                                    <NavTitleText>Ingestion</NavTitleText>
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
                                    <NavTitleText>Glossary</NavTitleText>
                                </NavTitleContainer>
                                <NavTitleDescription>View and modify your data dictionary</NavTitleDescription>
                            </Link>
                        </MenuItem>
                        {showTests && (
                            <MenuItem key="2">
                                <Link to="/tests">
                                    <NavTitleContainer>
                                        <FileDoneOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                        <NavTitleText>Tests</NavTitleText>
                                    </NavTitleContainer>
                                    <NavTitleDescription>
                                        Monitor policies & automate actions across data assets
                                    </NavTitleDescription>
                                </Link>
                            </MenuItem>
                        )}
                        {showDomains && (
                            <MenuItem key="1">
                                <Link to="/domains">
                                    <NavTitleContainer>
                                        <FolderOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                        <NavTitleText>Domains</NavTitleText>
                                    </NavTitleContainer>
                                    <NavTitleDescription>Manage related groups of data assets</NavTitleDescription>
                                </Link>
                            </MenuItem>
                        )}
                    </Menu>
                }
            >
                <LinkWrapper>
                    <Button type="text">
                        <SolutionOutlined /> Govern <DownOutlined style={{ fontSize: '6px' }} />
                    </Button>
                </LinkWrapper>
            </Dropdown>
            {showObserve && (
                <Dropdown
                    trigger={['click']}
                    overlay={
                        <Menu>
                            {showDatasetHealth && (
                                <MenuItem key="1">
                                    <Link to={PageRoutes.DATASET_HEALTH_DASHBOARD}>
                                        <NavTitleContainer>
                                            <StyledDatabaseOutlined />
                                            <NavTitleText>Dataset Health</NavTitleText>
                                        </NavTitleContainer>
                                        <NavTitleDescription>
                                            Monitor active incidents & failing assertions across your
                                            organization&apos;s datasets
                                        </NavTitleDescription>
                                    </Link>
                                </MenuItem>
                            )}
                        </Menu>
                    }
                >
                    <LinkWrapper>
                        <Button type="text">
                            <EyeOutlined /> Observe <DownOutlined style={{ fontSize: '6px' }} />
                        </Button>
                    </LinkWrapper>
                </Dropdown>
            )}
            {showSettings && (
                <LinkWrapper style={{ marginRight: 12 }}>
                    <Link to="/settings">
                        <Button type="text">
                            <Tooltip title="Manage your DataHub settings">
                                <SettingOutlined />
                            </Tooltip>
                        </Button>
                    </Link>
                </LinkWrapper>
            )}
        </LinksWrapper>
    );
}
