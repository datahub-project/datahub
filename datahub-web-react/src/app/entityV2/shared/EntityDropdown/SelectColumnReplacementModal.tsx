import { Modal, SimpleSelect } from '@components';
import { cloneDeep } from 'lodash';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Label } from '@components/components/Input/components';

import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { EntitySearchInputV2 } from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputV2';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity, EntityType } from '@types';

// Kept at module scope so each identity is stable: EntitySearchInputV2 re-issues its search whenever
// the array it is given changes.
const DATASET_SEARCH_TYPES = [EntityType.Dataset];
const GLOSSARY_TERM_SEARCH_TYPES = [EntityType.GlossaryTerm];

/**
 * Only dataset and glossaryTerm declare schemaMetadata (entity-registry.yml), so those are the only
 * parents a column can live in. A replacement is offered from the same kind of parent as the
 * deprecated column: terms for terms, datasets for datasets.
 */
const getSearchTypes = (parentEntityType: EntityType) =>
    parentEntityType === EntityType.GlossaryTerm ? GLOSSARY_TERM_SEARCH_TYPES : DATASET_SEARCH_TYPES;

const Fields = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const Field = styled.div`
    display: flex;
    flex-direction: column;
`;

type Props = {
    /** Entity type of the deprecated column's own parent, which the search is restricted to. */
    parentEntityType: EntityType;
    /** Parent the column list is populated from when the dialog opens. */
    initialTableUrn?: string;
    /** Field path to pre-select, when it belongs to initialTableUrn. */
    initialFieldPath?: string;
    /** Called with the assembled schemaField urn, or null when no column is selected. */
    onSave: (replacementUrn: string | null) => void;
    onCancel: () => void;
};

/**
 * Picks the column that replaces a deprecated one. The table is picked separately from the column so
 * the replacement can live in a different asset than the deprecated column.
 *
 * Selections are local until Save, so cancelling leaves an already-saved replacement alone.
 */
export default function SelectColumnReplacementModal({
    parentEntityType,
    initialTableUrn,
    initialFieldPath,
    onSave,
    onCancel,
}: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const parentEntityName = entityRegistry.getEntityName(parentEntityType);

    const [tableUrn, setTableUrn] = useState<string | undefined>(initialTableUrn);
    const [fieldPath, setFieldPath] = useState<string | undefined>(initialFieldPath);

    const { data: tableData } = useGetEntitiesQuery({
        variables: {
            urns: [initialTableUrn || ''],
        },
        skip: !initialTableUrn,
    });

    // Serves a glossary term parent as well: the `dataset` root field fetches aspects by urn without
    // checking the entity type, and dataset and glossaryTerm both declare schemaMetadata under that
    // name. Deliberately not useGetEntityWithSchema, which reads the urn from the surrounding entity
    // context — the deprecated column's table rather than the one selected here.
    const { data: schemaData, loading: schemaLoading } = useGetDatasetSchemaQuery({
        variables: {
            urn: tableUrn || '',
        },
        skip: !tableUrn,
        fetchPolicy: 'cache-first',
    });

    // A sibling dataset often carries the schema of the pair, so the columns of both are offered —
    // matching what useGetEntityWithSchema does for the schema tab, and what this picker inherited
    // from it before the parent became selectable.
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const parentSchemaData = useMemo(
        () => (schemaData && !isHideSiblingMode ? combineEntityDataWithSiblings(cloneDeep(schemaData)) : schemaData),
        [schemaData, isHideSiblingMode],
    );

    // Which side of a sibling pair declares each column. The urn has to name a parent that actually
    // carries the field, otherwise it points at a schemaField entity that was never materialised —
    // those are built from the schemaMetadata of whoever declares the column.
    const columnOwnerUrns = useMemo(() => {
        const owners = new Map<string, string>();
        (schemaData?.dataset?.siblingsSearch?.searchResults ?? []).forEach(({ entity }) => {
            if (!entity?.urn || !('schemaMetadata' in entity)) return;
            entity.schemaMetadata?.fields?.forEach((field) => owners.set(field.fieldPath, entity.urn));
        });
        // The selected parent wins whenever it declares the column itself.
        (schemaData?.dataset?.schemaMetadata?.fields ?? []).forEach(
            (field) => schemaData?.dataset?.urn && owners.set(field.fieldPath, schemaData.dataset.urn),
        );
        return owners;
    }, [schemaData]);

    const columnOptions = useMemo(() => {
        // Offer fields only from the schema of the parent selected right now: until it arrives the
        // list stays empty, so a urn can never pair a new parent with a field from the previous one.
        const parent = parentSchemaData?.dataset?.urn === tableUrn ? parentSchemaData?.dataset : undefined;
        return (parent?.schemaMetadata?.fields ?? []).map((field) => ({
            value: field.fieldPath,
            label: downgradeV2FieldPath(field.fieldPath) as string,
        }));
    }, [parentSchemaData, tableUrn]);

    const handleTableChange = (table?: Entity) => {
        // Picking the parent that is already selected is not a change: the search input reports every
        // click, including one on the current value while the pre-selection is still resolving.
        if (table?.urn === tableUrn) return;
        setTableUrn(table?.urn);
        // A field path from the previous table almost never exists in the new one, and keeping it
        // would build a urn pointing at a column that isn't there.
        setFieldPath(undefined);
    };

    return (
        <Modal
            title={t('deprecation.selectReplacement')}
            onCancel={onCancel}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onCancel,
                    buttonDataTestId: 'select-replacement-cancel',
                },
                {
                    text: tc('save'),
                    // No column selected means there is nothing to save: the parent alone does not
                    // make a replacement, and a parent change clears the column, so this covers both.
                    disabled: !fieldPath,
                    onClick: () =>
                        onSave(
                            generateSchemaFieldUrn(
                                fieldPath,
                                (fieldPath && columnOwnerUrns.get(fieldPath)) || tableUrn || '',
                            ),
                        ),
                    buttonDataTestId: 'select-replacement-save',
                },
            ]}
        >
            <Fields>
                <Field>
                    <Label>{parentEntityName}</Label>
                    <EntitySearchInputV2
                        entityTypes={getSearchTypes(parentEntityType)}
                        initialValue={tableData?.entities?.[0] ?? undefined}
                        placeholder={t('deprecation.replacementParentPlaceholder', {
                            entityName: parentEntityName,
                        })}
                        onUpdate={handleTableChange}
                    />
                </Field>
                <SimpleSelect
                    label={t('deprecation.replacementColumnLabel')}
                    placeholder={t('deprecation.replacementColumnPlaceholder')}
                    options={columnOptions}
                    isLoading={schemaLoading}
                    values={fieldPath ? [fieldPath] : []}
                    onUpdate={(values) => setFieldPath(values[0])}
                    width="full"
                    showSearch
                    dataTestId="deprecation-replacement-column"
                />
            </Fields>
        </Modal>
    );
}
