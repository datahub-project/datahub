import React from 'react';
import styled from 'styled-components';

import { getColor } from '@components/theme/utils';

import {
    PERSONA_TYPES_TO_DISPLAY,
    PERSONA_TYPE_TO_DESCRIPTION,
    PERSONA_TYPE_TO_LABEL,
    PERSONA_TYPE_TO_VIEW_ICON,
} from '@app/homeV2/shared/types';
import { Heading } from '@src/alchemy-components';

const PersonaCard = styled.div<{ selected: boolean }>`
    border: 1px rgb(217, 217, 217) solid;
    border-radius: 5px;
    padding: 12px;
    margin: 12px 0;
    display: flex;
    height: 88px;
    font-family: Mulish;

    &:hover {
        cursor: pointer;
        ${(props) => !props.selected && `border: 1.5px ${getColor('primary', 200, props.theme)} solid;`}
    }

    ${(props) => props.selected && `border: 1.5px ${props.theme.styles['primary-color']} solid;`}
`;
const StyledIcon = styled.div`
    display: flex;
    align-items: center;
    font-size: 20px;
    margin-left: 8px;
    margin-right: 20px;
`;

const PersonaSelectorContainer = styled.div`
    margin-top: 10px;
    max-width: 290px;
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    font-size: 11px;
`;

const Text = styled.div`
    color: rgb(129, 135, 159);
    line-height: 16px;
    font-size: 12px;
    margin-bottom: 4px;
`;

type Props = {
    onSelect: (urn: string) => void;
    selectedPersona: string;
};

export const PersonaSelector = ({ onSelect, selectedPersona }: Props) => {
    return (
        <PersonaSelectorContainer>
            <Heading size="md" type="h4" color="gray">
                Select a Persona
            </Heading>
            {PERSONA_TYPES_TO_DISPLAY.map((urn) => (
                <PersonaCard onClick={() => onSelect(urn)} key={urn} selected={urn === selectedPersona}>
                    <StyledIcon>{PERSONA_TYPE_TO_VIEW_ICON[urn]}</StyledIcon>
                    <Content>
                        <Heading size="md" type="h5" weight="semiBold">
                            {PERSONA_TYPE_TO_LABEL[urn]}
                        </Heading>
                        <Text>{PERSONA_TYPE_TO_DESCRIPTION[urn]}</Text>
                    </Content>
                </PersonaCard>
            ))}
        </PersonaSelectorContainer>
    );
};
