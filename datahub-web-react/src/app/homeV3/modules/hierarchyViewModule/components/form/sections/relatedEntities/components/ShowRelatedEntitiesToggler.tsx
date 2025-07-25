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
                <Text weight="bold" lineHeight="sm">
                    Show Related Entities
                </Text>
                <Text color="gray" lineHeight="sm">
                    Show related entities for this term. Toggling shows entities in widget.
                </Text>
            </LabelContainer>
            <Switch label="" isChecked={isChecked} onChange={() => onChange(!isChecked)} />
        </Wrapper>
    );
}
