import KeyboardArrowLeftIcon from '@mui/icons-material/KeyboardArrowLeft';
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight';
import React, { useEffect } from 'react';
import styled from 'styled-components';
import { SchemaField } from '../../../../../../../../types.generated';
import { pluralize } from '../../../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../../constants';

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
    align-items: center;
`;

interface Props {
    schemaFields?: SchemaField[];
    expandedFieldIndex?: number;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
}

export default function DrawerFooter({ schemaFields = [], expandedFieldIndex = 0, setExpandedDrawerFieldPath }: Props) {
    function showNextField() {
        if (expandedFieldIndex !== undefined && expandedFieldIndex !== -1) {
            if (expandedFieldIndex === schemaFields.length - 1) {
                const newField = schemaFields[0];
                setExpandedDrawerFieldPath(newField.fieldPath);
            } else {
                const newField = schemaFields[expandedFieldIndex + 1];
                const { fieldPath } = newField;
                setExpandedDrawerFieldPath(fieldPath);
            }
        }
    }

    function showPreviousField() {
        if (expandedFieldIndex !== undefined && expandedFieldIndex !== -1) {
            if (expandedFieldIndex === 0) {
                const newField = schemaFields[schemaFields.length - 1];
                setExpandedDrawerFieldPath(newField.fieldPath);
            } else {
                const newField = schemaFields[expandedFieldIndex - 1];
                setExpandedDrawerFieldPath(newField.fieldPath);
            }
        }
    }

    function handleArrowKeys(event: KeyboardEvent) {
        if (event.code === 'ArrowUp' || event.code === 'ArrowLeft') {
            showPreviousField();
        } else if (event.code === 'ArrowDown' || event.code === 'ArrowRight') {
            showNextField();
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleArrowKeys);

        return () => document.removeEventListener('keydown', handleArrowKeys);
    });

    return (
        <HeaderWrapper>
            <ButtonsWrapper>
                <StyledIcon onClick={showPreviousField}>
                    <KeyboardArrowLeftIcon />
                </StyledIcon>
                <FieldIndexText>
                    {expandedFieldIndex + 1} of {schemaFields.length} {pluralize(schemaFields.length, 'field')}
                </FieldIndexText>
                <StyledIcon onClick={showNextField}>
                    <KeyboardArrowRightIcon />
                </StyledIcon>
            </ButtonsWrapper>
        </HeaderWrapper>
    );
}
