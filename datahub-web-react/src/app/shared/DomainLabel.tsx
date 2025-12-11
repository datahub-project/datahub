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

const DomainContainerWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const DomainContentWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    name: string;
};

export const DomainLabel = ({ name }: Props) => {
    return (
        <DomainContainerWrapper>
            <DomainContentWrapper>
                <div>{name}</div>
            </DomainContentWrapper>
        </DomainContainerWrapper>
    );
};
