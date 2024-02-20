import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import LanguageIcon from '@mui/icons-material/Language';
import CloseIcon from '@mui/icons-material/Close';
import { ANTD_GRAY, SEARCH_COLORS } from '../../shared/constants';

const SelectButton = styled(Button)<{ isOpen: boolean }>`
    background-color: ${(props) => (props.isOpen ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    border-color: ${(props) => (props.isOpen ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    color: ${ANTD_GRAY[1]};
    &: hover {
        background: ${SEARCH_COLORS.TITLE_PURPLE};
        color: ${ANTD_GRAY[1]};
        border-color: ${(props) => (props.isOpen ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    }
    &: focus {
        background-color: ${(props) => (props.isOpen ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
        color: ${ANTD_GRAY[1]};
        border-color: ${(props) => (props.isOpen ? SEARCH_COLORS.TITLE_PURPLE : 'transparent')};
    }
`;

const SelectButtonContainer = styled.div`
    position: relative;
`;

const CloseButtonContainer = styled.div`
    position: absolute;
    top: 0px;
    right: 0px;
    background-color: ${ANTD_GRAY[1]};
    display: flex;
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
    isOpen: boolean;
    onClear: () => void;
};

export const renderSelectedView = ({ selectedViewName, isOpen, onClear }: Props) => {
    return (
        <SelectButtonContainer>
            <SelectButton isOpen={isOpen}>{selectedViewName || <LanguageIconStyle />}</SelectButton>
            {selectedViewName && isOpen && (
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
