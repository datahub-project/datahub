import { Editor, Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataProductBuilderState } from '@app/entityV2/domain/DataProductsTab/types';
import DataProductParentSelect from '@app/entityV2/shared/EntityDropdown/DataProductParentSelect';

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.bgHover};
`;

const FieldGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const FieldLabel = styled.div`
    margin-bottom: 4px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
`;

type Props = {
    builderState: DataProductBuilderState;
    updateBuilderState: (newState: DataProductBuilderState) => void;
    /** When editing, exclude this data product from the parent picker. */
    excludeUrn?: string;
};

export default function DataProductBuilderForm({ builderState, updateBuilderState, excludeUrn }: Props) {
    const { t: tl } = useTranslation('common.labels');
    const { t } = useTranslation('entity.types');

    function updateName(name: string) {
        updateBuilderState({
            ...builderState,
            name,
        });
    }

    function updateDescription(description: string) {
        updateBuilderState({
            ...builderState,
            description,
        });
    }

    function setSelectedParentUrn(parentDataProductUrn: string, parentDataProductName?: string) {
        updateBuilderState({
            ...builderState,
            parentDataProductUrn: parentDataProductUrn || undefined,
            parentDataProductName: parentDataProductUrn
                ? (parentDataProductName ?? builderState.parentDataProductName)
                : undefined,
        });
    }

    return (
        <FieldGroup>
            {/* eslint-disable i18next/no-literal-string -- (untranslated-text) example-value placeholder; illustrative sample text intentionally kept in EN */}
            <Input
                label={tl('name')}
                isRequired
                autoFocus
                value={builderState.name}
                setValue={updateName}
                placeholder="Revenue Dashboards"
                inputTestId="name-input"
            />
            {/* eslint-enable i18next/no-literal-string */}
            <div>
                <FieldLabel>{tl('description')}</FieldLabel>
                <StyledEditor doNotFocus content={builderState.description} onChange={updateDescription} />
            </div>
            <div>
                <FieldLabel>
                    {t('dataProduct.parentLabel')} {tl('optional')}
                </FieldLabel>
                <DataProductParentSelect
                    selectedParentUrn={builderState.parentDataProductUrn || ''}
                    setSelectedParentUrn={setSelectedParentUrn}
                    excludeUrn={excludeUrn}
                    initialParentName={builderState.parentDataProductName}
                />
            </div>
        </FieldGroup>
    );
}
