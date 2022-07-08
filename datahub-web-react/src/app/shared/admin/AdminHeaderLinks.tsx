import styled from 'styled-components';
import * as React from 'react';
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
import { Button, Dropdown, Menu } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

const AdminLink = styled.span`
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

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges.manageIngestion && me.platformPrivileges.manageSecrets;
    const showDomains = me?.platformPrivileges?.manageDomains || false;
    const showGlossary = me?.platformPrivileges?.manageGlossaries || false;

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
            {showIngestion && (
                <AdminLink>
                    <Link to="/ingestion">
                        <Button type="text">
                            <ApiOutlined /> Ingestion
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
