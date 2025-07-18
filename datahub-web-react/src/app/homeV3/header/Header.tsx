import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import GreetingText from '@app/homeV3/header/components/GreetingText';
import SearchBar from '@app/homeV3/header/components/SearchBar';
import { CenteredContainer } from '@app/homeV3/styledComponents';

export const HeaderWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 27px 40px 24px 40px;
    width: 100%;
    background: linear-gradient(180deg, #f8fcff 0%, #fafafb 100%);
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px 12px 0 0;
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
