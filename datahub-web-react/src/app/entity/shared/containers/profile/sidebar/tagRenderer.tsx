import React from 'react';
import { Tag } from 'antd';
import styled from 'styled-components';

const StyleTag = styled(Tag)`
    padding: 0px 7px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

export const tagRender = (props) => {
    // eslint-disable-next-line react/prop-types
    const { label, closable, onClose } = props;
    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };
    return (
        <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
            {label}
        </StyleTag>
    );
};
