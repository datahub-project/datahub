import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import LogicalModelPlatformSelect, {
    LOGICAL_PLATFORM_URN,
    PLATFORM_NAME_MAX_LENGTH,
    platformIdFromUrn,
} from '@app/entityV2/shared/logicalModels/LogicalModelPlatformSelect';
import LogicalModelSchemaBuilder from '@app/entityV2/shared/logicalModels/LogicalModelSchemaBuilder';
import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Input, Modal, toast } from '@src/alchemy-components';

import { useCreateLogicalModelMutation } from '@graphql/logical.generated';
import { EntityType, FabricType, SchemaFieldDataType } from '@types';

const Row = styled.div`
    display: flex;
    gap: 16px;
    align-items: flex-end;
    margin-bottom: 8px;

    /* Platform ~40%, Name ~60% */
    > *:first-child {
        flex: 2;
        min-width: 0;
    }

    > *:last-child {
        flex: 3;
        min-width: 0;
    }
`;

const FieldWrapper = styled.div`
    margin-bottom: 16px;
`;

const UrnPreview = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
    margin-bottom: 16px;
    word-break: break-all;
`;

const PlatformNameError = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textError};
    margin-top: -8px;
    margin-bottom: 8px;
`;

type Props = {
    onClose: () => void;
    onCreate?: (urn: string) => void;
};

export default function CreateLogicalModelModal({ onClose, onCreate }: Props) {
    const { t } = useTranslation('logicalModels');
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [createLogicalModel] = useCreateLogicalModelMutation();

    const [name, setName] = useState('');
    const [displayName, setDisplayName] = useState('');
    const [platformUrn, setPlatformUrn] = useState(LOGICAL_PLATFORM_URN);
    const [columns, setColumns] = useState<LogicalModelColumnDraft[]>([
        { fieldPath: '', type: SchemaFieldDataType.String },
    ]);
    const [submitting, setSubmitting] = useState(false);

    const validColumns = columns.filter((col) => col.fieldPath.trim().length > 0);
    const platformNameTooLong = platformIdFromUrn(platformUrn).length > PLATFORM_NAME_MAX_LENGTH;
    const canCreate = name.trim().length > 0 && validColumns.length > 0 && !platformNameTooLong;

    const onSubmit = () => {
        const cols = validColumns.map((col) => ({ fieldPath: col.fieldPath.trim(), type: col.type }));
        const paths = cols.map((col) => col.fieldPath);
        const duplicate = paths.find((path, i) => paths.indexOf(path) !== i);
        if (duplicate) {
            toast.error(t('column.duplicateError', { name: duplicate }), { duration: 3 });
            return;
        }
        setSubmitting(true);
        createLogicalModel({
            variables: {
                input: {
                    name: name.trim(),
                    displayName: displayName.trim() || undefined,
                    env: FabricType.Prod,
                    platform: platformUrn || undefined,
                    columns: cols,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors && data?.createLogicalModel) {
                    toast.success(t('modal.successMessage'), { duration: 2 });
                    onCreate?.(data.createLogicalModel);
                    history.push(entityRegistry.getEntityUrl(EntityType.Dataset, data.createLogicalModel));
                }
            })
            .catch((e) => toast.error(t('modal.errorMessage', { error: e.message }), { duration: 3 }))
            .finally(() => {
                setSubmitting(false);
                onClose();
            });
    };

    return (
        <Modal
            title={t('modal.title')}
            onCancel={onClose}
            buttons={[
                { text: t('modal.cancel'), variant: 'text', onClick: onClose },
                {
                    text: t('modal.create'),
                    variant: 'filled',
                    buttonDataTestId: 'create-logical-model-button',
                    disabled: !canCreate || submitting,
                    isLoading: submitting,
                    onClick: onSubmit,
                },
            ]}
        >
            <Row>
                <LogicalModelPlatformSelect
                    label={t('modal.platformLabel')}
                    value={platformUrn}
                    onChange={setPlatformUrn}
                />
                <Input
                    label={t('modal.nameLabel')}
                    isRequired
                    placeholder={t('modal.namePlaceholder')}
                    value={name}
                    setValue={setName}
                />
            </Row>
            {platformNameTooLong && (
                <PlatformNameError>
                    {t('modal.platformNameTooLong', { max: PLATFORM_NAME_MAX_LENGTH })}
                </PlatformNameError>
            )}
            <UrnPreview>
                {t('modal.urnPreviewHint', { platform: platformUrn, name: name || t('modal.namePlaceholder') })}
            </UrnPreview>
            <FieldWrapper>
                <Input label={t('modal.displayNameLabel')} value={displayName} setValue={setDisplayName} />
            </FieldWrapper>
            <FieldWrapper>
                <div>{t('modal.columnsLabel')}</div>
                <LogicalModelSchemaBuilder columns={columns} onChange={setColumns} />
            </FieldWrapper>
        </Modal>
    );
}
