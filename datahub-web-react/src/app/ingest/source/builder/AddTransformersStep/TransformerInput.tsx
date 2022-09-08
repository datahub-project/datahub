import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../../../../types.generated';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { EntitySearchInput } from '../../../../entity/shared/EntitySearchInput/EntitySearchInput';
import { Transformer, TransformerTypes } from '../types';

const TransformerWrapper = styled.div`
    // border-bottom: 1px dashed ${ANTD_GRAY[5]};
    // padding: 15px 0;
    padding: 12px 0;
`;

const StyledSelect = styled(Select)`
    min-width: 150px;
`;

const EntitySearchWrapper = styled.div`
    margin-top: 10px;

    .ant-select {
        width: 100%;
    }
`;

function getPlaceholderText(type: string | null) {
    let entityName = 'entities';
    if (type === TransformerTypes.ADD_OWNERS) entityName = 'owners';
    if (type === TransformerTypes.ADD_TAGS) entityName = 'tags';
    if (type === TransformerTypes.ADD_TERMS) entityName = 'glossary terms';
    if (type === TransformerTypes.ADD_DOMAIN) entityName = 'domains';
    return `Search for ${entityName}...`;
}

const typeOptions = [
    { value: TransformerTypes.ADD_OWNERS, label: 'Add Owners' },
    { value: TransformerTypes.ADD_TAGS, label: 'Add Tags' },
    { value: TransformerTypes.ADD_TERMS, label: 'Add Glossary Terms' },
    { value: TransformerTypes.ADD_DOMAIN, label: 'Add Domain' },
];

interface Props {
    transformer: Transformer;
    existingTransformerTypes: (string | null)[];
    index: number;
    setTransformers: React.Dispatch<React.SetStateAction<Transformer[]>>;
}

export default function TransformerInput(props: Props) {
    const { transformer, existingTransformerTypes, index, setTransformers } = props;

    function updateType(value: string) {
        setTransformers((prevTransformers) => {
            return prevTransformers.map((t, i) => {
                if (i === index) return { ...t, type: value };
                return t;
            });
        });
    }

    function updateUrns(urns: string[]) {
        setTransformers((prevTransformers) => {
            return prevTransformers.map((t, i) => {
                if (i === index) return { ...t, urns };
                return t;
            });
        });
    }

    return (
        <TransformerWrapper>
            <StyledSelect value={transformer.type || ''} onChange={(e) => updateType(e as string)}>
                {typeOptions.map((option) => (
                    <Select.Option
                        key={option.value}
                        disabled={existingTransformerTypes.includes(option.value)}
                        value={option.value}
                    >
                        {option.label}
                    </Select.Option>
                ))}
            </StyledSelect>
            {transformer.type && (
                <EntitySearchWrapper>
                    <EntitySearchInput
                        selectedUrns={transformer.urns}
                        placeholder={getPlaceholderText(transformer.type)}
                        entityTypes={[EntityType.Tag]}
                        onChangeSelectedUrns={updateUrns}
                    />
                </EntitySearchWrapper>
            )}
        </TransformerWrapper>
    );
}
