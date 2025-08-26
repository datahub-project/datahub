import { QuestionCircleOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu } from 'antd';
import * as React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useTheme } from 'styled-components/macro';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { useHandleOnboardingTour } from '@app/onboarding/useHandleOnboardingTour';
import { MenuItem } from '@app/shared/admin/components';
import { useAppConfig } from '@app/useAppConfig';

const TourContainer = styled.div``;

export default function HelpDropdown() {
    const themeConfig = useTheme();
    const me = useUserContext();
    const { config } = useAppConfig();
    const { helpLinkState } = useGlobalSettingsContext();
    const { showOnboardingTour } = useHandleOnboardingTour();
    const { isEnabled: isHelpLinkEnabled, label, link } = helpLinkState;
    const version = config?.appVersion;
    const showAddHelpLink = !isHelpLinkEnabled && me.platformPrivileges?.manageGlobalSettings;

    return (
        <Dropdown
            trigger={['click']}
            overlay={
                <Menu>
                    {isHelpLinkEnabled && (
                        <>
                            <MenuItem key="customHelp">
                                <a href={link} target="_blank" rel="noopener noreferrer">
                                    {label}
                                </a>
                            </MenuItem>
                            <Menu.Divider />
                        </>
                    )}
                    <MenuItem key="productTour">
                        <TourContainer onClick={showOnboardingTour}>Product Tour</TourContainer>
                    </MenuItem>
                    {themeConfig.content.menu.items.map((value) => {
                        return (
                            <MenuItem key={value.label}>
                                <a
                                    href={value.path || ''}
                                    target={value.shouldOpenInNewTab ? '_blank' : ''}
                                    rel="noopener noreferrer"
                                    tabIndex={0}
                                >
                                    {value.label}
                                </a>
                            </MenuItem>
                        );
                    })}
                    <MenuItem key="graphiQLLink">
                        <a href="/api/graphiql" target="_blank" rel="noopener noreferrer">
                            GraphiQL
                        </a>
                    </MenuItem>
                    <MenuItem key="openapiLink">
                        <a href="/openapi/swagger-ui/index.html" target="_blank" rel="noopener noreferrer">
                            OpenAPI
                        </a>
                    </MenuItem>
                    {version && (
                        <MenuItem key="version" disabled style={{ color: '#8C8C8C' }}>
                            {version}
                        </MenuItem>
                    )}
                    {showAddHelpLink && (
                        <>
                            <Menu.Divider />
                            <MenuItem key="addCustomHelp">
                                <Link to="/settings/helpLink">Add Custom Help Link</Link>
                            </MenuItem>
                        </>
                    )}
                </Menu>
            }
        >
            <Button type="text">
                <QuestionCircleOutlined />
            </Button>
        </Dropdown>
    );
}
