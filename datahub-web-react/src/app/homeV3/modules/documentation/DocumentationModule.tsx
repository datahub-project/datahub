import React from 'react';
import styled from 'styled-components';

import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

const StyledEditor = styled(Editor)`
    border: none;
    &&& {
        .remirror-editor {
            padding: 0;
        }
    }
`;

const DocumentationModule = (props: ModuleProps) => {
    const content = props.module.properties.params.richTextParams?.content;
    return (
        <LargeModule {...props}>
            <StyledEditor content={content} readOnly />
        </LargeModule>
    );
};

export default DocumentationModule;
