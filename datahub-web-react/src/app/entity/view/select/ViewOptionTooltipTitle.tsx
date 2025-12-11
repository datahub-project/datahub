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

const NameContainer = styled.div`
    margin-bottom: 8px;
`;

type Props = {
    name: string;
    description?: string | null;
};

export const ViewOptionTooltipTitle = ({ name, description }: Props) => {
    return (
        <>
            <NameContainer>
                <b>{name}</b>
            </NameContainer>
            <div>{description}</div>
        </>
    );
};
