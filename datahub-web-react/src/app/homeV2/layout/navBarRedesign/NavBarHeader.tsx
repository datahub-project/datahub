import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarToggler from '@app/homeV2/layout/navBarRedesign/NavBarToggler';
import { useGetFontSizeForContainer } from '@app/homeV2/layout/navBarRedesign/useGetFontSizeForContainer';
import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';
import { useIsHomePage } from '@app/shared/useIsHomePage';
import { ThemeId, useCustomThemeId } from '@app/useSetAppTheme';
import { colors } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';

import DatahubCloudLogo from '@images/datahub_cloud.svg?react';

const LETTER_LOGO_GAP = '16px';
const LETTER_LOGO_MARGIN = '0 0 4px 0';

const Container = styled.div`
    display: flex;
    width: 100%;
    height: 40px;
    min-height: 40px;
    align-items: center;
    gap: 8px;
    margin-left: -3px;
    transition: padding 250ms ease-in-out;
`;

const Logotype = styled.div<{ $margin?: string }>`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 24px;
    max-height: 24px;
    max-width: 42px;
    border-radius: 4px;
    position: relative;
    object-fit: contain;
    ${({ $margin }) => $margin && `margin: ${$margin};`}

    & svg,
    img {
        max-height: 24px;
        max-width: 42px;
        min-width: 42px;
        object-fit: contain;
    }
`;

const Title = styled.div<{ $fontSize: number }>`
    color: ${colors.gray[1700]};
    font-style: normal;
    font-weight: 700;
    font-family: Mulish;
    font-size: ${({ $fontSize }) => $fontSize}px;
    text-wrap: nowrap;
    white-space: nowrap;
    overflow: hidden;
    max-width: calc(100% - 30px);
    display: flex;
    align-items: end;
`;

const StyledLink = styled(Link)<{ $gap?: string }>`
    display: flex;
    height: 40px;
    align-items: center;
    max-width: calc(100% - 40px);
    width: 100%;
    gap: ${({ $gap }) => $gap || '8px'};
`;

type Props = {
    logotype?: React.ReactElement;
};

export default function NavBarHeader({ logotype }: Props) {
    const { toggle, isCollapsed } = useNavBarContext();
    const { globalSettings } = useGlobalSettingsContext();
    const showHomepageRedesign = useShowHomePageRedesign();
    const customName = globalSettings?.visualSettings?.customOrgName;
    const isHomePage = useIsHomePage();
    // start SaaS Only - custom styling for specific customer logos :(
    const customThemeId = useCustomThemeId();
    const hasLetterLogo = customThemeId === ThemeId.FIS;
    // end SaaS Only

    const { containerRef: titleRef, fontSize } = useGetFontSizeForContainer(customName || undefined, [isCollapsed]);

    function handleLogoClick() {
        if (isHomePage && showHomepageRedesign) {
            toggle();
        }
        analytics.event({ type: EventType.NavBarItemClick, label: 'Home' });
    }

    return (
        <Container>
            <StyledLink
                to="/"
                onClick={handleLogoClick}
                $gap={hasLetterLogo ? LETTER_LOGO_GAP : undefined}
                data-testid="nav-bar-home-logo"
            >
                <Logotype $margin={hasLetterLogo ? LETTER_LOGO_MARGIN : undefined}>{logotype}</Logotype>
                {!isCollapsed && !customName && <DatahubCloudLogo />}
                {!isCollapsed && customName && (
                    <Title ref={titleRef} $fontSize={fontSize}>
                        {customName}
                    </Title>
                )}
            </StyledLink>
            {!isCollapsed && <NavBarToggler />}
        </Container>
    );
}
