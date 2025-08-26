import { Skeleton } from 'antd';
import React from 'react';
import { FcHighPriority, FcLeave, FcMediumPriority } from 'react-icons/fc';
import styled from 'styled-components';

import { Body, PrimaryHeading } from '@app/govern/Dashboard/components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const Container = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    background-color: #fff;
    height: 100%;
`;

const ChartStateCard = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0.5rem;
    margin-top: 0.5rem;
    height: 100%;
    width: 100%;
    background-color: #f5f5f5;
    border-radius: 8px;

    svg {
        margin-right: 0.25rem;
    }
`;

const FlexWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 1;

    p:not(:first-child) {
        font-size: 16px;
        margin-bottom: 0;
    }
`;

// Whole section waiting (waterfall render)
export const SectionWaiting = () => <Skeleton title={false} paragraph={{ rows: 1, width: '100%' }} active />;

// Loading, no data, and error states
// TODO: Improve loading state
export const ChartLoading = () => <Skeleton title={false} paragraph={{ rows: 1, width: '100%' }} active />;

// No data for this time frame
export const ChartNoDataTimeframe = () => (
    <ChartStateCard>
        <FcLeave size={18} />
        No data for this time frame.
    </ChartStateCard>
);

// Not enough data to display helpful information
export const ChartNotEnoughData = () => <ChartStateCard>Not enough data to calculate a trend.</ChartStateCard>;

// No data retrieved (just.. nothing)
export const ChartNoData = () => (
    <ChartStateCard>
        <FcMediumPriority size={18} />
        No data received.
    </ChartStateCard>
);

// An error occured
export const ChartError = () => (
    <ChartStateCard>
        <FcHighPriority size={18} />
        An error occured.
    </ChartStateCard>
);

// View if the Integration Service is offline
export const IntegrationServiceOffline = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Container>
            <Body $isShowNavBarRedesign={isShowNavBarRedesign}>
                <FlexWrapper>
                    <div style={{ textAlign: 'center', fontSize: '18px' }}>
                        <PrimaryHeading>Your Compliance Form Initiatives</PrimaryHeading>
                        <p style={{ marginTop: '1rem' }}>Oops! It seems like compliance form metrics are missing 🤔</p>
                        <p>
                            Either you haven&apos;t set up your compliance forms and reporting, or our systems are down.
                        </p>
                        <p>Please try again later!</p>
                    </div>
                </FlexWrapper>
            </Body>
        </Container>
    );
};

// View if the user doesn't have the required permissions
export const MissingPermissions = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Container>
            <Body $isShowNavBarRedesign={isShowNavBarRedesign}>
                <FlexWrapper>
                    <div style={{ textAlign: 'center', fontSize: '18px' }}>
                        <PrimaryHeading>Your Compliance Form Initiatives</PrimaryHeading>
                        <p style={{ marginTop: '1rem' }}>
                            Oops! It seems like you don&apos;t have permission to view this page
                        </p>
                        <p>Contact your admin to gain access.</p>
                    </div>
                </FlexWrapper>
            </Body>
        </Container>
    );
};

const LoadingPermissionsWrapper = styled.div`
    text-align: center;
    font-size: 18px;
    width: 500px;
`;

const LoadingPermissionsSpacer = styled.div`
    margin-top: 20px;
`;

// View if permissions are still loading
export const LoadingPermissions = () => (
    <Container>
        <Body>
            <FlexWrapper>
                <LoadingPermissionsWrapper>
                    <PrimaryHeading>Loading</PrimaryHeading>
                    <LoadingPermissionsSpacer />
                    <Skeleton title={false} paragraph={{ rows: 4 }} active />
                    <Skeleton title={false} paragraph={{ rows: 4 }} active />
                    <Skeleton title={false} paragraph={{ rows: 4 }} active />
                </LoadingPermissionsWrapper>
            </FlexWrapper>
        </Body>
    </Container>
);

export const ChartState = ({
    loading,
    error,
    noDataTimeframe,
    noData,
}: {
    loading: boolean;
    error: boolean;
    noDataTimeframe: boolean;
    noData: boolean;
}) => {
    if (!loading && error) return <ChartError />;
    if (!loading && !error && noDataTimeframe) return <ChartNoDataTimeframe />;
    if (!loading && !error && !noDataTimeframe && noData) return <ChartNoData />;
    return <ChartLoading />;
};
