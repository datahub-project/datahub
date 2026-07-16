import React from 'react';
import styled from 'styled-components';

import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarToggler from '@app/homeV2/layout/navBarRedesign/NavBarToggler';
import GreetingText from '@app/homeV3/header/components/GreetingText';
import SearchBar from '@app/homeV3/header/components/SearchBar';
import { CenteredContainer, contentWidth } from '@app/homeV3/styledComponents';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const HeaderWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 27px 0 24px 0;
    width: 100%;
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px 12px 0 0;
    position: relative;
`;

const StyledCenteredContainer = styled(CenteredContainer)`
    padding: 0 43px;
    ${contentWidth(0)}
`;

const NavTogglerSlot = styled.div`
    position: absolute;
    top: 16px;
    left: 16px;
`;

const Header = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { isCollapsed } = useNavBarContext();
    const showHomeNavToggler = isShowNavBarRedesign && isCollapsed;

    return (
        <HeaderWrapper>
            {showHomeNavToggler && (
                <NavTogglerSlot>
                    <NavBarToggler />
                </NavTogglerSlot>
            )}
            <StyledCenteredContainer>
                <GreetingText />
                <SearchBar />
            </StyledCenteredContainer>
        </HeaderWrapper>
    );
};

export default Header;
