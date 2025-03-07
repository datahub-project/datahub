import React from 'react';
import styled from 'styled-components';
import { Editor } from '../../../tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY_V2 } from '../../../constants';

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY_V2[6]};
    min-height: 115px;
    border-radius: 6px;
    width: 75%;
    min-width: 585px;
    max-width: 700px;
    max-height: 300px;
    overflow: auto;

    &&& {
        .remirror-editor {
            padding: 16px 24px;
        }
    }
`;

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function RichTextInput({ selectedValues, updateSelectedValues }: Props) {
    function updateInput(value: string) {
        updateSelectedValues([value]);
    }

    return (
        <StyledEditor
            doNotFocus
            content={selectedValues.length > 0 ? selectedValues[0] : undefined}
            onChange={updateInput}
        />
    );
}
