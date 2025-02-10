import React from 'react';
import styled from 'styled-components';
import NumericDistributionChart from './NumericDistributionChart/NumericDistributionChart';

const FooterSpace = styled.div`
    // Add extra footer space to handle overlay by filds switcher (DrawerFooter)
    height: 80px;
`;

export default function ChartsSection() {
    return (
        <>
            <NumericDistributionChart />
            <FooterSpace />
        </>
    );
}
