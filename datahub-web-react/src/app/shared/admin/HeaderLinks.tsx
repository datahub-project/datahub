import {
    ApiOutlined,
    BarChartOutlined,
    BookOutlined,
    DatabaseOutlined,
    DownOutlined,
    EyeOutlined,
    FileDoneOutlined,
    FormOutlined,
    GlobalOutlined,
    InboxOutlined,
    SettingOutlined,
    SolutionOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import { Button, Dropdown, Menu } from 'antd';
import { Tooltip } from '@components';
import * as React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { PageRoutes } from '../../../conf/Global';
import { useUserContext } from '../../context/useUserContext';
import DomainIcon from '../../domain/DomainIcon';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useToggleEducationStepIdsAllowList } from '../../onboarding/useToggleEducationStepIdsAllowList';
import { useAppConfig, useBusinessAttributesFlag, useIsDocumentationFormsEnabled } from '../../useAppConfig';
import HelpDropdown from './HelpDropdown';
import { TaskCenterLink } from './TaskCenterLink';

const LinkWrapper = styled.span`
    margin-right: 0px;

    span {
        padding: 0;
    }
`;

const LinksWrapper = styled.div<{ areLinksHidden?: boolean }>`
    display: flex;
    align-items: center;
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
    font-weight: bold;
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
    const { showFormAnalytics, formCreationEnabled } = config.featureFlags;
    const isDocumentationFormsEnabled = useIsDocumentationFormsEnabled();

    const businessAttributesFlag = useBusinessAttributesFlag();

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
        isIngestionEnabled && me && (me.platformPrivileges?.manageIngestion || me.platformPrivileges?.manageSecrets);

    // SaaS only
    const showActionRequests =
        (!isDocumentationFormsEnabled && isActionRequestsEnabled && me?.platformPrivileges?.viewMetadataProposals) ||
        false;
    const showTests = (isTestsEnabled && me?.platformPrivileges?.manageTests) || false;
    const showDatasetHealth = config?.featureFlags?.datasetHealthDashboardEnabled;
    const showObserve = showDatasetHealth;
    const showDocumentationCenter =
        config?.featureFlags?.documentationFormsEnabled &&
        (me.platformPrivileges?.manageDocumentationForms || me.platformPrivileges?.viewDocumentationFormsPage) &&
        (showFormAnalytics || formCreationEnabled);
    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    useToggleEducationStepIdsAllowList(!!showIngestion, HOME_PAGE_INGESTION_ID);

    const items = [
        {
            key: 0,
            label: (
                <Link to="/glossary">
                    <NavTitleContainer>
                        <BookOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                        <NavTitleText>Glossary</NavTitleText>
                    </NavTitleContainer>
                    <NavTitleDescription>View and modify your data dictionary</NavTitleDescription>
                </Link>
            ),
        },
        {
            key: 1,
            label: (
                <Link to="/domains">
                    <NavTitleContainer>
                        <DomainIcon
                            style={{
                                fontSize: 14,
                                fontWeight: 'bold',
                            }}
                        />
                        <NavTitleText>Domains</NavTitleText>
                    </NavTitleContainer>
                    <NavTitleDescription>Manage related groups of data assets</NavTitleDescription>
                </Link>
            ),
        },
        ...(businessAttributesFlag
            ? [
                  {
                      key: 2,
                      label: (
                          <Link to="/business-attribute">
                              <NavTitleContainer>
                                  <GlobalOutlined
                                      style={{
                                          fontSize: 14,
                                          fontWeight: 'bold',
                                      }}
                                  />
                                  <NavTitleText>Business Attribute</NavTitleText>
                              </NavTitleContainer>
                              <NavTitleDescription>Universal field for data consistency</NavTitleDescription>
                          </Link>
                      ),
                  },
              ]
            : []),
        ...(showTests
            ? [
                  {
                      key: 3,
                      label: (
                          <Link to="/tests">
                              <NavTitleContainer>
                                  <FileDoneOutlined
                                      style={{
                                          fontSize: 14,
                                          fontWeight: 'bold',
                                      }}
                                  />
                                  <NavTitleText>Tests</NavTitleText>
                              </NavTitleContainer>
                              <NavTitleDescription>
                                  Monitor policies & automate actions across data assets
                              </NavTitleDescription>
                          </Link>
                      ),
                  },
              ]
            : []),
        ...(showDocumentationCenter
            ? [
                  {
                      key: 4,
                      label: (
                          <Link to="/govern/dashboard">
                              <NavTitleContainer>
                                  <FormOutlined
                                      style={{
                                          fontSize: 14,
                                          fontWeight: 'bold',
                                      }}
                                  />
                                  <NavTitleText>Compliance Forms</NavTitleText>
                              </NavTitleContainer>
                              <NavTitleDescription>
                                  Manage compliance initiatives for your data assets
                              </NavTitleDescription>
                          </Link>
                      ),
                  },
              ]
            : []),
        ...(showStructuredProperties
            ? [
                  {
                      key: 5,
                      label: (
                          <Link to={PageRoutes.STRUCTURED_PROPERTIES}>
                              <NavTitleContainer>
                                  <UnorderedListOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                  <NavTitleText>Structured Properties</NavTitleText>
                              </NavTitleContainer>
                              <NavTitleDescription>Manage custom properties for your data assets</NavTitleDescription>
                          </Link>
                      ),
                  },
              ]
            : []),
    ];

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
            {isDocumentationFormsEnabled && <TaskCenterLink />}
            <Dropdown trigger={['click']} menu={{ items }}>
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
            <HelpDropdown />
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
