import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { ArrowLeft as ArrowLeftIcon, ArrowRight as ArrowRightIcon } from 'phosphor-react';

export const BulkNavigationWrapper = styled.div<{ $hideBackground?: boolean }>`
    padding: 16px 68px 16px 24px;
    display: flex;
    justify-content: flex-start;
    border-bottom: 1px solid ${colors.gray[100]};
    ${(props) =>
        !props.$hideBackground &&
        `
        background-position: right;
        background-repeat: no-repeat;
    `}
`;

export const NavigationWrapper = styled.div<{ isHidden: boolean }>`
    font-size: 16px;
    font-weight: bold;
    color: ${colors.gray[600]};
    display: flex;
    flex-wrap: nowrap;
    ${(props) => props.isHidden && 'opacity: 0;'}
    align-items: center;
`;

export const ArrowLeft = styled(ArrowLeftIcon)<{ $shouldHide?: boolean }>`
    margin-right: 16px;
    cursor: pointer;
    ${(props) => props.$shouldHide && 'visibility: hidden;'}
`;

export const ArrowRight = styled(ArrowRightIcon)<{ $shouldHide?: boolean }>`
    margin-left: 16px;
    cursor: pointer;
    ${(props) => props.$shouldHide && 'visibility: hidden;'}
`;
