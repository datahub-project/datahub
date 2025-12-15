import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import FreeTrialDaysLeft from '@app/homeV3/freeTrial/FreeTrialDaysLeft';
import GreetingText from '@app/homeV3/header/components/GreetingText';
import SearchBar from '@app/homeV3/header/components/SearchBar';
import { CenteredContainer, contentWidth } from '@app/homeV3/styledComponents';
import { useIsFreeTrialInstance } from '@app/useAppConfig';

export const HeaderWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 27px 0 24px 0;
    width: 100%;
    background: linear-gradient(180deg, #f8fcff 0%, #fafafb 100%);
    border-bottom: 1px solid ${colors.gray[100]};
    border-radius: 12px 12px 0 0;
    position: relative;
`;

const StyledCenteredContainer = styled(CenteredContainer)`
    padding: 0 43px;
    ${contentWidth(0)}
`;

export const GreetingInfoContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const Header = () => {
    const isFreeTrialInstance = useIsFreeTrialInstance();
    const showFreeTrialMessage = isFreeTrialInstance;

    return (
        <HeaderWrapper>
            <StyledCenteredContainer>
                <GreetingInfoContainer>
                    <GreetingText />
                    {showFreeTrialMessage && <FreeTrialDaysLeft />}
                </GreetingInfoContainer>
                <SearchBar />
            </StyledCenteredContainer>
        </HeaderWrapper>
    );
};

export default Header;
