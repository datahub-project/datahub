/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tag } from 'antd';
import React from 'react';
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
