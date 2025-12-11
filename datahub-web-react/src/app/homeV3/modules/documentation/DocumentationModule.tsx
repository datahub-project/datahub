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

import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

const StyledEditor = styled(Editor)`
    border: none;
    padding: 8px;
    &&& {
        .remirror-editor {
            padding: 0;
        }
    }
`;

const DocumentationModule = (props: ModuleProps) => {
    const content = props.module.properties.params.richTextParams?.content;
    return (
        <LargeModule {...props} dataTestId="documentation-module">
            <StyledEditor content={content} readOnly />
        </LargeModule>
    );
};

export default DocumentationModule;
