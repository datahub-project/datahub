import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

const StyledEditor = styled(Editor)<{ displayBulkPromptStyles?: boolean }>`
    border: 1px solid ${ANTD_GRAY_V2[6]};
    min-height: 115px;
    border-radius: 6px;
    width: 75%;
    min-width: 585px;
    max-width: 700px;
    max-height: 300px;
    overflow: auto;

    ${(props) =>
        props.displayBulkPromptStyles &&
        `
    background-color: white;
    max-height: 160px;
    overflow: auto;
    `}

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
    const {
        prompt: { displayBulkPromptStyles },
    } = useEntityFormContext();

    function updateInput(value: string) {
        updateSelectedValues([value]);
    }

    return (
        <StyledEditor
            doNotFocus
            content={selectedValues.length > 0 ? selectedValues[0] : undefined}
            onChange={updateInput}
            displayBulkPromptStyles={displayBulkPromptStyles}
        />
    );
}
