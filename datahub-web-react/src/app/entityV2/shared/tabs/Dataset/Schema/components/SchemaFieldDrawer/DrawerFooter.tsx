import React from 'react';
import styled from 'styled-components';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import { pluralize } from '../../../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../../constants';
import { ExtendedSchemaFields } from '../../../../../../dataset/profile/schema/utils/types';

const HeaderWrapper = styled.div`
    position: absolute;
    align-self: flex-end;
    display: flex;
    justify-content: center;
    align-self: center;
    margin-bottom: 16px;
    padding: 2px 4px;
    background: #d9d9d9;
    opacity: 0.8;
    border-radius: 15.5px;
    width: max-content;
    color: ${REDESIGN_COLORS.DARK_GREY};
    bottom: 0px;
`;

const StyledIcon = styled.div`
    font-size: 12px;
    padding: 0;
    height: 26px;
    width: 26px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: transparent;
    border: none;

    &&:hover {
        cursor: pointer;
        stroke: ${REDESIGN_COLORS.DARK_GREY};
        stroke-width: 1px;
    }

    svg {
        height: 20px;
        width: 20px;
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

const FieldIndexText = styled.span`
    font-size: 12px;
    font-weight: 800;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
    margin: 0 8px;
`;

const ButtonsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

interface Props {
    expandedFieldIndex?: number;
    selectPreviousField: () => void;
    selectNextField: () => void;
    displayedRows: ExtendedSchemaFields[];
}

export default function DrawerFooter({
    expandedFieldIndex = 0,
    selectPreviousField,
    selectNextField,
    displayedRows,
}: Props) {
    return (
        <HeaderWrapper>
            <ButtonsWrapper>
                <StyledIcon onClick={selectPreviousField}>
                    <KeyboardArrowUpIcon />
                </StyledIcon>
                <FieldIndexText>
                    {expandedFieldIndex + 1} of {displayedRows.length} {pluralize(displayedRows.length, 'field')}
                </FieldIndexText>
                <StyledIcon onClick={selectNextField}>
                    <KeyboardArrowDownIcon />
                </StyledIcon>
            </ButtonsWrapper>
        </HeaderWrapper>
    );
}
