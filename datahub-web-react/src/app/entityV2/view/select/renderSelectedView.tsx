import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { Tooltip } from '@components';
import { colors } from '@src/alchemy-components';
import { FadersHorizontal } from '@phosphor-icons/react';
import CloseIcon from '@mui/icons-material/Close';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../../shared/constants';
import { ViewLabel } from './styledComponents';

const SelectButton = styled(Button)<{ $selectedViewName: string; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return props.$selectedViewName ? colors.gray[1000] : 'transparent';
        }
        return props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent';
    }};
    border-color: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return props.$selectedViewName ? 'transparent' : colors.gray[100];
        }
        return props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent';
    }};
    color: ${(props) => (props.$isShowNavBarRedesign ? colors.violet[500] : ANTD_GRAY[1])};
    max-width: ${(props) => (props.$isShowNavBarRedesign ? '120px' : '150px')};

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 28px;
        padding: 3px 8px;
        display: flex;
        box-shadow: none;
        line-height: 20px;

        & svg {
            color: ${REDESIGN_COLORS.GREY_300};
            transition: all 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
        }
    `}

    &: hover {
        background: ${(props) => {
            if (props.$isShowNavBarRedesign) {
                return props.$selectedViewName ? colors.gray[1000] : 'transparent';
            }
            return SEARCH_COLORS.TITLE_PURPLE;
        }};
        color: ${(props) => (props.$isShowNavBarRedesign ? colors.violet[500] : ANTD_GRAY[1])};

        border-color: ${(props) => {
            if (props.$isShowNavBarRedesign) return colors.violet[500];
            return props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent';
        }};
    }

    &: focus {
        background-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
        color: ${(props) => (props.$isShowNavBarRedesign ? colors.violet[500] : ANTD_GRAY[1])};
        border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};

        ${(props) =>
            props.$isShowNavBarRedesign &&
            `
            background-color: ${colors.gray[1000]};

            & svg {
                color: ${REDESIGN_COLORS.GREY_300};
            }
        `}
    }
`;

const SelectButtonContainer = styled.div`
    position: relative;

    &&&& .close-container {
        display: none;
    }

    &:hover,
    &:focus {
        &&&& .close-container {
            display: flex;
        }
    }
`;

const CloseButtonContainer = styled.div`
    position: absolute;
    top: 0px;
    right: 0px;
    background-color: ${ANTD_GRAY[1]};
    align-items: center;
    border-radius: 100%;
    padding: 5px;
    cursor: pointer;
`;

const CloseIconStyle = styled(CloseIcon)`
    font-size: 10px !important;
    color: ${SEARCH_COLORS.TITLE_PURPLE};
`;

const StyledViewIcon = styled(FadersHorizontal)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '20px' : '18px')} !important;
    color: ${ANTD_GRAY[1]};
`;

type Props = {
    selectedViewName: string;
    isShowNavBarRedesign?: boolean;
    onClear: () => void;
};

export const renderSelectedView = ({ selectedViewName, isShowNavBarRedesign, onClear }: Props) => {
    return (
        <SelectButtonContainer>
            <SelectButton $selectedViewName={selectedViewName} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <Tooltip showArrow={false} title={selectedViewName} placement="bottom">
                    <ViewLabel data-testid="views-icon">
                        {selectedViewName || <StyledViewIcon $isShowNavBarRedesign={isShowNavBarRedesign} />}
                    </ViewLabel>
                </Tooltip>
            </SelectButton>
            {selectedViewName && (
                <CloseButtonContainer
                    className="close-container"
                    onClick={(e) => {
                        e.stopPropagation();
                        onClear();
                    }}
                >
                    <CloseIconStyle />
                </CloseButtonContainer>
            )}
        </SelectButtonContainer>
    );
};
