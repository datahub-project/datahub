/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

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
