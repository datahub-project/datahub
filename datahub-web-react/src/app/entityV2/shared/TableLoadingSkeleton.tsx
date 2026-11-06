import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const Header = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    padding-right: 40px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const Body = styled.div``;

const HeaderSkeleton = styled(Skeleton.Input)`
    margin: 0px 12px 12px 0px;
    border-radius: 8px;
`;

const CardWrapper = styled.div`
    padding: 20px 40px;
`;

const CardSkeleton = styled(Skeleton.Input)`
    margin: 0px 12px 12px 0px;
    border-radius: 8px;
`;

export const TableLoadingSkeleton = () => {
    return (
        <>
            <Header>
                <HeaderSkeleton size="lg" width={540} height={60} />
            </Header>
            <Body>
                <CardWrapper>
                    <CardSkeleton size="lg" height={60} block />
                </CardWrapper>
                <CardWrapper>
                    <CardSkeleton size="lg" height={60} block />
                </CardWrapper>
                <CardWrapper>
                    <CardSkeleton size="lg" height={60} block />
                </CardWrapper>
            </Body>
        </>
    );
};
