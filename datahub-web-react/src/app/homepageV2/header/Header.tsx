import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import GreetingText from '@app/homepageV2/header/components/GreetingText';
import SearchBar from '@app/homepageV2/header/components/SearchBar';

export const HeaderWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 27px 0 24px 0;
    width: 100%;
    overflow: hidden;
    background: linear-gradient(180deg, #f8fcff 0%, #fafafb 100%);
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px 12px 0 0;
`;

const CenteredContainer = styled.div`
    max-width: 1016px;
    width: 100%;
`;

const Header = () => {
    return (
        <HeaderWrapper>
            <CenteredContainer>
                <GreetingText />
                <SearchBar />
            </CenteredContainer>
        </HeaderWrapper>
    );
};

export default Header;
