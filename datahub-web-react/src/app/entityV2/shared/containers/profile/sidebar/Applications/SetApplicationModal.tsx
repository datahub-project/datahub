import { Modal, Select, Typography, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useBatchSetApplicationMutation, useGetApplicationsListQuery } from '@graphql/application.generated';
import { Application, EntityType } from '@types';

interface Props {
    urns: string[];
    onCloseModal: () => void;
    refetch?: () => void;
}

export const SetApplicationModal = ({ urns, onCloseModal, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const [applicationUrn, setApplicationUrn] = useState<string | undefined>(undefined);
    const { data, loading, error } = useGetApplicationsListQuery({
        variables: {
            input: {
                start: 0,
                count: 1000,
                query: '',
                types: [EntityType.Application],
            },
        },
    });
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();

    const onOk = () => {
        if (!applicationUrn) {
            return;
        }
        batchSetApplicationMutation({
            variables: {
                input: {
                    applicationUrn,
                    resourceUrns: urns,
                },
            },
        })
            .then(() => {
                message.success({ content: t('sidebar.application.setSuccess'), duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: t('sidebar.application.setFailed', { message: e.message || '' }),
                        duration: 3,
                    });
                }
            })
            .finally(() => {
                onCloseModal();
            });
    };

    const applicationOptions =
        data?.searchAcrossEntities?.searchResults
            ?.map((r) => r.entity)
            .filter((entity): entity is Application => entity.__typename === 'Application')
            .map((appEntity) => {
                return {
                    value: appEntity.urn,
                    label: appEntity.properties?.name || '',
                };
            }) || [];

    return (
        <Modal title={t('sidebar.application.modalTitle')} open onOk={onOk} onCancel={onCloseModal} closable>
            <Select
                showSearch
                style={{ width: '100%' }}
                placeholder={t('sidebar.application.selectPlaceholder')}
                onChange={(value) => setApplicationUrn(value)}
                filterOption={(input, option) => (option?.label ?? '').toLowerCase().includes(input.toLowerCase())}
                options={applicationOptions}
                loading={loading}
            />
            {error && (
                <Typography.Text type="danger">
                    {t('sidebar.application.loadFailed', { message: error.message })}
                </Typography.Text>
            )}
        </Modal>
    );
};
