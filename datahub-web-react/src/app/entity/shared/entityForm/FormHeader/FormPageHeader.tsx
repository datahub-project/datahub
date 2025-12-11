/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import AppLogoLink from '@app/shared/AppLogoLink';

const Header = styled.div`
    padding: 12px 24px;
    background-color: black;
    font-size: 24px;
    display: flex;
    align-items: center;
    color: white;
    justify-content: space-between;
`;

const HeaderText = styled.div`
    margin-left: 24px;
`;

const StyledDivider = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

export default function FormPageHeader() {
    return (
        <StyledDivider>
            <Header>
                <TitleWrapper>
                    <AppLogoLink />
                    <HeaderText>Complete Documentation Requests</HeaderText>
                </TitleWrapper>
            </Header>
        </StyledDivider>
    );
}
