import React from 'react';
import styled from 'styled-components/macro';

import { useEntityRegistry } from '@app/useEntityRegistry';

const AttributeWrapper = styled.div`
    font-weight: normal;
    margin-bottom: 4px;
    color: ${(props) => props.theme.colors.text};
`;

const nameStyles = `
    color: inherit;
    display: inline-block;
    height: 100%;
    padding: 3px 4px;
    width: 100%;
`;

export const NameWrapper = styled.span<{ showSelectStyles?: boolean }>`
    ${nameStyles}

    &:hover {
        ${(props) =>
            props.showSelectStyles &&
            `
                background-color: ${props.theme.colors.bgSurface};
                cursor: pointer;
        `}
    }
`;

interface Props {
    attribute: any;
    isSelecting?: boolean;
    selectAttribute?: (urn: string, displayName: string) => void;
}

function AttributeItem(props: Props) {
    const { attribute, isSelecting, selectAttribute } = props;

    const entityRegistry = useEntityRegistry();

    function handleSelectAttribute() {
        if (selectAttribute) {
            const displayName = entityRegistry.getDisplayName(attribute.type, attribute);
            selectAttribute(attribute.urn, displayName);
        }
    }

    return (
        <AttributeWrapper>
            {isSelecting && (
                <NameWrapper showSelectStyles={!!selectAttribute} onClick={handleSelectAttribute}>
                    {entityRegistry.getDisplayName(attribute.type, attribute)}
                </NameWrapper>
            )}
        </AttributeWrapper>
    );
}

export default AttributeItem;
