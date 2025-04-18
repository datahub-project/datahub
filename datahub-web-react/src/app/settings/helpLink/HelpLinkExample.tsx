import React from 'react';
import styled from 'styled-components';
import HelpExampleImage from '../../../images/customHelpExample.svg';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';

const HelpExampleWrapper = styled.div`
    flex: 1;
    padding-left: 80px;
`;

const ContentWrapper = styled.div`
    width: 287px;
    font-size: 12px;
    color: ${ANTD_GRAY_V2[8]};

    img {
        margin-bottom: 8px;
    }
`;

export default function HelpLinkExample() {
    return (
        <HelpExampleWrapper>
            <ContentWrapper>
                <img src={HelpExampleImage} alt="help-example" />
                <span>Your Custom Help Link will appear under the Help Menu in the navigation as shown here.</span>
            </ContentWrapper>
        </HelpExampleWrapper>
    );
}
