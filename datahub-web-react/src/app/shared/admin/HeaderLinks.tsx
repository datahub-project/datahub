import styled from 'styled-components/macro';
import * as React from 'react';
import {
    ApiOutlined,
    BarChartOutlined,
    BookOutlined,
    SettingOutlined,
    SolutionOutlined,
    DownOutlined,
    GlobalOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button, Dropdown, Tooltip } from 'antd';
import { useAppConfig, useBusinessAttributesFlag } from '../../useAppConfig';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useToggleEducationStepIdsAllowList } from '../../onboarding/useToggleEducationStepIdsAllowList';
import { PageRoutes } from '../../../conf/Global';
import { useUserContext } from '../../context/useUserContext';
import DomainIcon from '../../domain/DomainIcon';

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

const NavTitleContainer = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
    padding: 2px;
`;

const NavSubItemTitleContainer = styled(NavTitleContainer)`
    line-height: 20px;
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

interface Props {
    areLinksHidden?: boolean;
}

export function HeaderLinks(props: Props) {
    const { areLinksHidden } = props;
    const me = useUserContext();
    const { config } = useAppConfig();

    const businessAttributesFlag = useBusinessAttributesFlag();

    const isAnalyticsEnabled = config?.analyticsConfig?.enabled;
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && (me.platformPrivileges?.manageIngestion || me.platformPrivileges?.manageSecrets);
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
        ...(showStructuredProperties
            ? [
                  {
                      key: 5,
                      label: (
                          <Link to={PageRoutes.STRUCTURED_PROPERTIES}>
                              <NavSubItemTitleContainer>
                                  <UnorderedListOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} />
                                  <NavTitleText>Structured Properties</NavTitleText>
                              </NavSubItemTitleContainer>
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
            <Dropdown trigger={['click']} menu={{ items }}>
                <LinkWrapper>
                    <Button type="text">
                        <SolutionOutlined /> Govern <DownOutlined style={{ fontSize: '6px' }} />
                    </Button>
                </LinkWrapper>
            </Dropdown>
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
