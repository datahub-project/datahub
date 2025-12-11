/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Switch, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
`;

const LabelContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
`;

interface Props {
    isChecked: boolean;
    onChange: (isChecked: boolean) => void;
}

export default function ShowRelatedEntitiesSwitch({ isChecked, onChange }: Props) {
    return (
        <Wrapper>
            <LabelContainer>
                <Text weight="bold" color="gray" colorLevel={600} lineHeight="sm">
                    Show Related Entities
                </Text>
                <Text color="gray" lineHeight="sm">
                    Show related entities for this term. Toggling shows entities in the module.
                </Text>
            </LabelContainer>
            <Switch label="" isChecked={isChecked} onChange={() => onChange(!isChecked)} />
        </Wrapper>
    );
}
