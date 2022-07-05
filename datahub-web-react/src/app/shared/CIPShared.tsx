import * as React from 'react';
import { PhaseBanner } from 'govuk-react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

export const FixedCIPHeader = styled.header`
    position: fixed;
    z-index: 10;
    width: 100%;
    borderbottom: 1px solid ${ANTD_GRAY[4.5]};
    background: white;
`;

const CIPBannerBoundary = styled.div`
    width: 100%;
    padding-left: 5px;
    padding-right: 5px;
`;

export const CIPBanner = () => {
    return (
        <>
            <CIPBannerBoundary>
                <PhaseBanner level="beta">
                    This is a new service – your
                    <a href="mailto: cipsupport@hmrc.gov.uk"> feedback</a> will help us to improve it.
                </PhaseBanner>
            </CIPBannerBoundary>
        </>
    );
};
