import React from 'react';
import styled from 'styled-components';
import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
import { ANTD_GRAY } from '../../shared/constants';
import { ViewContainer, ViewContent, ViewDescription, ViewIcon, ViewLabel } from './styledComponents';
import { Carousel } from '../../../shared/carousel/Carousel';

const StyledCarousel = styled(Carousel)`
    gap: 10px;
    padding: 20px 0;

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
    return (
        <StyledCarousel>
            <ViewContainer onClick={() => onClickCreateView()} role="none">
                <ViewIcon className="static">
                    <AddOutlinedIconStyle />
                </ViewIcon>
                <ViewContent>
                    <ViewLabel className="static">Create a View</ViewLabel>
                    <ViewDescription>Create view</ViewDescription>
                </ViewContent>
            </ViewContainer>
            {children}
        </StyledCarousel>
    );
};
