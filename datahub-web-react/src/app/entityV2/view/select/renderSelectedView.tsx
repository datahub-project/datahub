import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import LanguageIcon from '@mui/icons-material/Language';
import CloseIcon from '@mui/icons-material/Close';
import { ANTD_GRAY, SEARCH_COLORS } from '../../shared/constants';
import { ViewLabel } from './styledComponents';

const SelectButton = styled(Button)<{ $selectedViewName: string }>`
    background-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    color: ${ANTD_GRAY[1]};
    max-width: 150px;

    &: hover {
        background: ${SEARCH_COLORS.TITLE_PURPLE};
        color: ${ANTD_GRAY[1]};
        border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    }

    &: focus {
        background-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
        color: ${ANTD_GRAY[1]};
        border-color: ${(props) => (props.$selectedViewName ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
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

const LanguageIconStyle = styled(LanguageIcon)`
    font-size: 18px !important;
    color: ${ANTD_GRAY[1]};
`;

type Props = {
    selectedViewName: string;
    onClear: () => void;
};

export const renderSelectedView = ({ selectedViewName, onClear }: Props) => {
    return (
        <SelectButtonContainer>
            <SelectButton $selectedViewName={selectedViewName}>
                <Tooltip showArrow={false} title={selectedViewName} placement="bottom">
                    <ViewLabel>{selectedViewName || <LanguageIconStyle />}</ViewLabel>
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
