/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BookOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

type Props = {
    name: string;
};

const TermName = styled.span`
    margin-left: 5px;
`;

export default function TermLabel({ name }: Props) {
    return (
        <div>
            <BookOutlined />
            <TermName>{name}</TermName>
        </div>
    );
}
