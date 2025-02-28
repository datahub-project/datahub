import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';

const AttributeWrapper = styled.div`
    font-weight: normal;
    margin-bottom: 4px;
`;

const nameStyles = `
    color: #262626;
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
                background-color: ${ANTD_GRAY[3]};
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
