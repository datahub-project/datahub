import styled from 'styled-components';
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
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button, Dropdown, Menu } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

const AdminLink = styled.span`
    &&& .ant-btn-text {
        color: ${(props) => props.theme.styles['heading-color']};
        :hover {
            color: ${(props) => props.theme.styles['primary-color']};
        }
    }
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
`;

interface Props {
    areLinksHidden?: boolean;
}

export function AdminHeaderLinks(props: Props) {
    const { areLinksHidden } = props;
    const me = useGetAuthenticatedUser();
    const { config } = useAppConfig();

    const isAnalyticsEnabled = config?.analyticsConfig.enabled;
    const isIngestionEnabled = config?.managedIngestionConfig.enabled;
    // SaaS Only
    // Currently we only have a flag for metadata proposals.
    // In the future, we may add configs for alerts, announcements, etc.
    const isActionRequestsEnabled = config?.actionRequestsConfig.enabled;
    const isTestsEnabled = config?.testsConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges.manageIngestion && me.platformPrivileges.manageSecrets;
    const showDomains = me?.platformPrivileges?.manageDomains || false;
    const showGlossary = me?.platformPrivileges?.manageGlossaries || false;
    // SaaS only
    const showActionRequests = (isActionRequestsEnabled && me && me.platformPrivileges.viewMetadataProposals) || false;
    const showTests = (isTestsEnabled && me?.platformPrivileges?.manageTests) || false;

    return (
        <LinksWrapper areLinksHidden={areLinksHidden}>
            {showAnalytics && (
                <AdminLink>
                    <Link to="/analytics">
                        <Button type="text">
                            <BarChartOutlined /> Analytics
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showActionRequests && (
                <AdminLink>
                    <Link to="/requests">
                        <Button type="text">
                            <InboxOutlined /> My Requests
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showIngestion && (
                <AdminLink>
                    <Link to="/ingestion">
                        <Button type="text">
                            <ApiOutlined /> Ingestion
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showTests && (
                <AdminLink style={{ marginRight: 16 }}>
                    <Link to="/tests">
                        <Button type="text">
                            <FileDoneOutlined /> Tests
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {(showGlossary || showDomains) && (
                <Dropdown
                    trigger={['click']}
                    overlay={
                        <Menu>
                            {showGlossary && (
                                <MenuItem key="0">
                                    <Link to="/glossary">
                                        <BookOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} /> Glossary
                                    </Link>
                                </MenuItem>
                            )}
                            {showDomains && (
                                <MenuItem key="1">
                                    <Link to="/domains">
                                        <FolderOutlined style={{ fontSize: '14px', fontWeight: 'bold' }} /> Domains
                                    </Link>
                                </MenuItem>
                            )}
                        </Menu>
                    }
                >
                    <AdminLink>
                        <Button type="text">
                            <SolutionOutlined /> Govern <DownOutlined style={{ fontSize: '6px' }} />
                        </Button>
                    </AdminLink>
                </Dropdown>
            )}
            {showSettings && (
                <AdminLink style={{ marginRight: 12 }}>
                    <Link to="/settings">
                        <Button type="text">
                            <SettingOutlined />
                        </Button>
                    </Link>
                </AdminLink>
            )}
        </LinksWrapper>
    );
}
