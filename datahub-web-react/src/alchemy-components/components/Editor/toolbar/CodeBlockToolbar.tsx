import React from 'react';
import styled from 'styled-components';
import { Select } from 'antd';
import { findParentNodeOfType, isElementDomNode } from '@remirror/core';
import { defaultAbsolutePosition, hasStateChanged, isPositionVisible, Positioner } from 'remirror/extensions';
import { FloatingWrapper, useAttrs, useCommands } from '@remirror/react';
import { listLanguages } from 'refractor';
import { ToolbarContainer } from './FloatingToolbar';

const StyledSelect = styled(Select)`
    min-width: 120px;
`;

const codeBlockPositioner = Positioner.create<any>({
    hasChanged: hasStateChanged,

    getActive(props) {
        const { selection } = props.state;
        const codeBlock = findParentNodeOfType({ selection, types: 'codeBlock' });

        if (!codeBlock) {
            return Positioner.EMPTY;
        }

        return [codeBlock];
    },

    getPosition(props) {
        const { view, data: codeBlock } = props;

        const node = view.nodeDOM(codeBlock.pos);

        if (!isElementDomNode(node)) {
            return defaultAbsolutePosition;
        }

        const rect = node.getBoundingClientRect();
        const editorRect = view.dom.getBoundingClientRect();

        const { width, height } = rect;

        const left = view.dom.scrollLeft + rect.left - editorRect.left - 1;
        const top = view.dom.scrollTop + rect.top - editorRect.top - 1;

        return {
            x: left,
            y: top,
            height,
            width,
            rect,
            visible: isPositionVisible(rect, view.dom),
        };
    },
});

export const CodeBlockMenu = () => {
    const commands = useCommands();
    const value = (useAttrs(true).codeBlock()?.language as string) ?? 'markup';

    const options = listLanguages().map((language) => ({ value: language }));

    const onChange = (language) => {
        commands.updateCodeBlock({ language });
        commands.focus();
    };

    return (
        <ToolbarContainer>
            <StyledSelect
                value={value}
                size="small"
                options={options}
                onChange={onChange}
                dropdownMatchSelectWidth={100}
                showSearch
            />
        </ToolbarContainer>
    );
};

export const CodeBlockToolbar = () => (
    <FloatingWrapper positioner={codeBlockPositioner} placement="top-start">
        <CodeBlockMenu />
    </FloatingWrapper>
);
