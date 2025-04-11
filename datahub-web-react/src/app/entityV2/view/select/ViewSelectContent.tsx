import React from 'react';
import styled from 'styled-components';
import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Tooltip } from '@components';
import { ANTD_GRAY } from '../../shared/constants';
import {
    ViewContainer,
    ViewContent,
    ViewDescription,
    ViewIcon,
    ViewIconNavBarRedesign,
    ViewLabel,
} from './styledComponents';
import { Carousel } from '../../../sharedV2/carousel/Carousel';
import { ViewOptionTooltipTitle } from './ViewOptionTooltipTitle';

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

const AddOutlinedIconStyle = styled(AddOutlinedIcon)`
    font-size: 18px !important;
`;

type Props = {
    children: React.ReactNode;
    onClickCreateView: () => void;
};

export const ViewSelectContent = ({ children, onClickCreateView }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const IconWrapper = isShowNavBarRedesign ? ViewIconNavBarRedesign : ViewIcon;

    const renderCreateItem = () => {
        const name = 'Create a View';
        const description = isShowNavBarRedesign ? 'Create a set of saved search filters' : 'Create view';
        const ViewContentWrapper = isShowNavBarRedesign
            ? ({ children: tooltipChildren }) => (
                  <Tooltip
                      placement="bottom"
                      showArrow
                      title={<ViewOptionTooltipTitle name={name} description={description} />}
                  >
                      {tooltipChildren}
                  </Tooltip>
              )
            : React.Fragment;

        return (
            <ViewContainer onClick={() => onClickCreateView()} role="none" $isShowNavBarRedesign={isShowNavBarRedesign}>
                <IconWrapper>
                    <AddOutlinedIconStyle />
                </IconWrapper>
                <ViewContentWrapper>
                    <ViewContent $isShowNavBarRedesign={isShowNavBarRedesign}>
                        <ViewLabel className="static" $isShowNavBarRedesign={isShowNavBarRedesign}>
                            Create a View
                        </ViewLabel>
                        <ViewDescription $isShowNavBarRedesign={isShowNavBarRedesign}>
                            {isShowNavBarRedesign ? 'Create a set of saved search filters' : 'Create view'}
                        </ViewDescription>
                    </ViewContent>
                </ViewContentWrapper>
            </ViewContainer>
        );
    };

    return (
        <StyledCarousel $isShowNavBarRedesign={isShowNavBarRedesign}>
            {renderCreateItem()}
            {children}
        </StyledCarousel>
    );
};
