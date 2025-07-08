import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import CreateViewButton from '@app/entityV2/view/select/components/CreateViewButton';
import { Carousel } from '@app/sharedV2/carousel/Carousel';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const StyledCarousel = styled(Carousel)<{ $isShowNavBarRedesign?: boolean }>`
    gap: ${(props) => (props.$isShowNavBarRedesign ? '8px' : '10px')};
    padding: ${(props) => (props.$isShowNavBarRedesign ? '12px 8px 0 0' : '20px 0')};

    .rc-virtual-list-holder-inner {
        display: flex;
        flex-direction: row !important;
        gap: 1rem;
        .ant-select-item-option-content {
            display: flex;
            gap: 1rem;
            color: ${ANTD_GRAY[1]};
            padding: 10px 0px;
        }
        .ant-select-item,
        .ant-select-item-option-active:not(.ant-select-item-option-disabled),
        .ant-select-item-option-selected:not(.ant-select-item-option-disabled) {
            background: unset;
            padding: unset;
        }
    }
`;

type Props = {
    children: React.ReactNode;
    onClickCreateView: () => void;
};

export const ViewSelectContent = ({ children, onClickCreateView }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <StyledCarousel $isShowNavBarRedesign={isShowNavBarRedesign}>
            <CreateViewButton onClick={onClickCreateView} />
            {children}
        </StyledCarousel>
    );
};
