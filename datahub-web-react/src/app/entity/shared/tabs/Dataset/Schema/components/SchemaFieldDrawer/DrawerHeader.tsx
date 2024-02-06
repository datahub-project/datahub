import { CaretLeftOutlined, CaretRightOutlined, CloseOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../../../constants';
import { SchemaField } from '../../../../../../../../types.generated';
import { pluralize } from '../../../../../../../shared/textUtil';

const HeaderWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY_V2[4]};
    display: flex;
    justify-content: space-between;
    padding: 8px 16px;
`;

const StyledButton = styled(Button)`
    font-size: 12px;
    padding: 0;
    height: 26px;
    width: 26px;
    display: flex;
    align-items: center;
    justify-content: center;

    svg {
        height: 10px;
        width: 10px;
    }
`;

const FieldIndexText = styled.span`
    font-size: 14px;
    color: ${ANTD_GRAY_V2[8]};
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

export default function DrawerHeader({ schemaFields = [], expandedFieldIndex = 0, setExpandedDrawerFieldPath }: Props) {
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
                <StyledButton onClick={showPreviousField}>
                    <CaretLeftOutlined />
                </StyledButton>
                <FieldIndexText>
                    {expandedFieldIndex + 1} of {schemaFields.length} {pluralize(schemaFields.length, 'field')}
                </FieldIndexText>
                <StyledButton onClick={showNextField}>
                    <CaretRightOutlined />
                </StyledButton>
            </ButtonsWrapper>
            <StyledButton onClick={() => setExpandedDrawerFieldPath(null)}>
                <CloseOutlined />
            </StyledButton>
        </HeaderWrapper>
    );
}
