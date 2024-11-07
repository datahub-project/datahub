import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { Tooltip } from '@components';
import LanguageIcon from '@mui/icons-material/Language';
import CloseIcon from '@mui/icons-material/Close';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../../shared/constants';
import { ViewLabel } from './styledComponents';

const SelectButton = styled(Button)<{ $selectedViewName: string; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    border-color: ${(props) => {
        if (props.$selectedViewName) return SEARCH_COLORS.TITLE_PURPLE;
        return props.$isShowNavBarRedesign ? REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1 : 'transparent';
    }};
    color: ${ANTD_GRAY[1]};
    max-width: 150px;

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 28px;
        padding: 4px 8px;
        display: flex;
        box-shadow: none;
        line-height: 20px;

        & svg {
            color: ${REDESIGN_COLORS.GREY_300};
            transition: all 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
        }
    `}

    &: hover {
        background: ${SEARCH_COLORS.TITLE_PURPLE};
        color: ${ANTD_GRAY[1]};

        ${(props) =>
            props.$isShowNavBarRedesign &&
            `
            & svg {
                color: ${ANTD_GRAY[1]};
            }
        `}

        border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    }

    &: focus {
        background-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
        color: ${ANTD_GRAY[1]};
        border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};

        ${(props) =>
            props.$isShowNavBarRedesign &&
            `
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

const LanguageIconStyle = styled(LanguageIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '20px' : '18px')} !important;
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
                    <ViewLabel>
                        {selectedViewName || <LanguageIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />}
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
