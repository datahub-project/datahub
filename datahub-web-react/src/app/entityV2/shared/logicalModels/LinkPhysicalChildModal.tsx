import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import ColumnMappingEditor, { ColumnMapping } from '@app/entityV2/shared/logicalModels/ColumnMappingEditor';
import { Modal, toast } from '@src/alchemy-components';

import { useGetDatasetSchemaLazyQuery, useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { useGetDatasetPlatformLogicalLazyQuery, useLinkPhysicalChildMutation } from '@graphql/logical.generated';
import { EntityType } from '@types';

const Section = styled.div`
    margin-bottom: 16px;
`;

type Props = {
    logicalParentUrn: string;
    onClose: () => void;
    onLinked?: () => void;
};

export default function LinkPhysicalChildModal({ logicalParentUrn, onClose, onLinked }: Props) {
    const { t } = useTranslation('logicalModels');
    const [childUrn, setChildUrn] = useState<string | undefined>(undefined);
    const [childColumns, setChildColumns] = useState<string[]>([]);
    const [mappings, setMappings] = useState<ColumnMapping[]>([]);
    const [submitting, setSubmitting] = useState(false);
    const [linkPhysicalChild] = useLinkPhysicalChildMutation();
    const [fetchSchema] = useGetDatasetSchemaLazyQuery();
    const [fetchPlatform] = useGetDatasetPlatformLogicalLazyQuery();

    // The logical parent's own schema drives the mapping rows. entityData in the sidebar does not
    // reliably include schemaMetadata (the schema tab loads it lazily), so fetch it here directly.
    const { data: parentSchemaData, error: parentSchemaError } = useGetDatasetSchemaQuery({
        variables: { urn: logicalParentUrn },
        skip: !logicalParentUrn,
    });
    const parentColumns = (parentSchemaData?.dataset?.schemaMetadata?.fields ?? []).map((field) => field.fieldPath);

    // Surface a failed parent-schema load instead of silently rendering an empty mapping editor.
    useEffect(() => {
        if (parentSchemaError) {
            toast.error(t('link.schemaLoadError'), { duration: 3 });
        }
    }, [parentSchemaError, t]);

    const onSelectChild = async (urns: string[]) => {
        const urn = urns?.[0];
        if (!urn) {
            setChildUrn(undefined);
            setMappings([]);
            setChildColumns([]);
            return;
        }
        // Guard: a logical model (a dataset on any logical platform) must not be a physical child
        // of another logical model.
        try {
            const { data } = await fetchPlatform({ variables: { urn } });
            if (data?.dataset?.platform?.properties?.logical) {
                toast.error(t('link.invalidChildPlatform'), { duration: 3 });
                return;
            }
        } catch {
            toast.error(t('link.schemaLoadError'), { duration: 3 });
            return;
        }
        setChildUrn(urn);
        setMappings([]);
        setChildColumns([]);
        fetchSchema({ variables: { urn } })
            .then(({ data }) => {
                const fields = data?.dataset?.schemaMetadata?.fields ?? [];
                setChildColumns(fields.map((field) => field.fieldPath));
            })
            .catch(() => toast.error(t('link.schemaLoadError'), { duration: 3 }));
    };

    const onSubmit = () => {
        if (!childUrn) return;
        setSubmitting(true);
        linkPhysicalChild({
            variables: { input: { logicalParentUrn, childUrn, columnMappings: mappings } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('link.successMessage'), { duration: 2 });
                    onLinked?.();
                }
            })
            .catch((e) => toast.error(t('link.errorMessage', { error: e.message }), { duration: 3 }))
            .finally(() => {
                setSubmitting(false);
                onClose();
            });
    };

    return (
        <Modal
            title={t('link.title')}
            onCancel={onClose}
            buttons={[
                { text: t('modal.cancel'), variant: 'text', onClick: onClose },
                {
                    text: t('link.linkButton'),
                    variant: 'filled',
                    buttonDataTestId: 'link-physical-child-button',
                    disabled: !childUrn || submitting,
                    isLoading: submitting,
                    onClick: onSubmit,
                },
            ]}
        >
            <Section>
                <EntitySearchSelect
                    entityTypes={[EntityType.Dataset]}
                    selectedUrns={childUrn ? [childUrn] : []}
                    placeholder={t('link.pickChildPlaceholder')}
                    onUpdate={onSelectChild}
                    width="fit-content"
                />
            </Section>
            {childUrn && (
                <Section>
                    <div>{t('link.mapColumnsLabel')}</div>
                    <ColumnMappingEditor
                        parentColumns={parentColumns}
                        childColumns={childColumns}
                        value={mappings}
                        onChange={setMappings}
                    />
                </Section>
            )}
        </Modal>
    );
}
