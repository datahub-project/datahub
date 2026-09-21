import React from 'react';
import styled from 'styled-components';

import { Editor } from '@src/alchemy-components/components/Editor/Editor';

// The Editor draws no border of its own, so this box is ours to own.
const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
    min-height: 115px;
    border-radius: 6px;
    width: 100%;
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
