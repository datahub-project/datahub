import { Button, DatePicker, Form, message, Modal, Select, Skeleton } from 'antd';
import TextArea from 'antd/lib/input/TextArea';
import dayjs from 'dayjs';
import React from 'react';
import { useGetEntitiesQuery } from '../../../../graphql/entity.generated';
import { useBatchUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import { ResourceRefInput, SubResourceType } from '../../../../types.generated';
import { EntityLink } from '../../../homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '../../../lineageV2/lineageUtils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { downgradeV2FieldPath } from '../../dataset/profile/schema/utils/utils';
import { EntityCapabilityType } from '../../Entity';
import { SearchSelectModal } from '../components/styled/search/SearchSelectModal';
import { useGetEntityWithSchema } from '../tabs/Dataset/Schema/useGetEntitySchema';
import { generateSchemaFieldUrn } from '../tabs/Lineage/utils';
import { handleBatchError } from '../utils';

type Props = {
    urns: string[];
    // if you need to provide context for subresources, resourceRefs should be provided and will take precedence over urns
    resourceRefs?: ResourceRefInput[];
    onClose: () => void;
    refetch?: () => void;
    zIndexOverride?: number;
};

const SCHEMA_FIELD_PREFIX = 'urn:li:schemaField:';

export const UpdateDeprecationModal = ({ urns, resourceRefs, onClose, refetch, zIndexOverride }: Props) => {
    const { entityWithSchema } = useGetEntityWithSchema();
    const schemaMetadata: any = entityWithSchema?.schemaMetadata || undefined;

    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const [isReplacementModalVisible, setIsReplacementModalVisible] = React.useState(false);
    const [replacementUrn, setReplacementUrn] = React.useState<string | null>(null);
    const entityRegistry = useEntityRegistry();

    const isDeprecatingFields =
        !!resourceRefs && resourceRefs.length > 0 && resourceRefs[0].subResourceType === SubResourceType.DatasetField;
    const resourceFromWhichReplacementIsSelected = resourceRefs?.[0]?.resourceUrn;

    const { data: replacementData, loading: replacementLoading } = useGetEntitiesQuery({
        variables: {
            urns: [replacementUrn || ''],
        },
        skip: !replacementUrn || replacementUrn?.startsWith(SCHEMA_FIELD_PREFIX),
    });

    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: 'Updating...' });
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: resourceRefs || urns.map((resourceUrn) => ({ resourceUrn })),
                        deprecated: true,
                        note: formData.note,
                        decommissionTime: formData.decommissionTime && formData.decommissionTime.unix() * 1000,
                        replacement: replacementUrn,
                    },
                },
            });
            message.destroy();
            message.success({ content: 'Deprecation Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to update Deprecation: \n ${e.message || ''}`,
                        duration: 2,
                    }),
                );
            }
        }
        refetch?.();
        handleClose();
    };

    return (
        <Modal
            title="Set as Deprecated"
            zIndex={zIndexOverride ?? 10}
            visible
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" data-testid="add" form="addDeprecationForm" key="submit" htmlType="submit">
                        Save
                    </Button>
                </>
            }
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label="Reason" rules={[{ whitespace: true }, { min: 0, max: 1000 }]}>
                    <TextArea placeholder="Add Reason" autoFocus rows={4} />
                </Form.Item>
                <Form.Item name="decommissionTime" label="Decommission Date" initialValue={dayjs()}>
                    {/* @ts-expect-error dayjs type mismatch with DatePicker defaultValue */}
                    <DatePicker style={{ width: '100%' }} defaultValue={dayjs()} />
                </Form.Item>
                <Form.Item name="replacement" label="Replacement">
                    {isReplacementModalVisible && !isDeprecatingFields && (
                        <SearchSelectModal
                            limit={1}
                            titleText="Select one entity to replace the deprecated entity with."
                            continueText="Set Replacement"
                            onContinue={(entityUrns) => {
                                if (entityUrns.length > 0) {
                                    setReplacementUrn(entityUrns[0]);
                                }
                                setIsReplacementModalVisible(false);
                            }}
                            onCancel={() => setIsReplacementModalVisible(false)}
                            fixedEntityTypes={Array.from(
                                entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DEPRECATION),
                            )}
                        />
                    )}
                    {isReplacementModalVisible && isDeprecatingFields && (
                        <Modal
                            open
                            title="Select Replacement"
                            onCancel={() => setIsReplacementModalVisible(false)}
                            onOk={() => setIsReplacementModalVisible(false)}
                        >
                            <Select
                                style={{ width: 250 }}
                                dropdownMatchSelectWidth
                                placeholder="Select Replacement"
                                onChange={(value) =>
                                    setReplacementUrn(
                                        generateSchemaFieldUrn(value, resourceFromWhichReplacementIsSelected || ''),
                                    )
                                }
                            >
                                {schemaMetadata?.fields?.map((field: any) => (
                                    <Select.Option key={field.fieldPath} value={field.fieldPath}>
                                        {downgradeV2FieldPath(field.fieldPath)}
                                    </Select.Option>
                                ))}
                            </Select>
                        </Modal>
                    )}
                    {replacementUrn && replacementLoading && <Skeleton />}
                    {replacementUrn && !replacementLoading && !!replacementData?.entities?.[0] && (
                        <EntityLink
                            onClick={() => {
                                setIsReplacementModalVisible(true);
                            }}
                            entity={replacementData?.entities?.[0] as any}
                        />
                    )}
                    {replacementUrn && isDeprecatingFields && (
                        <Button
                            type="text"
                            style={{
                                padding: 5,
                                marginLeft: -5,
                            }}
                            onClick={() => {
                                setIsReplacementModalVisible(true);
                            }}
                        >
                            {getV1FieldPathFromSchemaFieldUrn(replacementUrn)}
                        </Button>
                    )}
                    {!replacementUrn && (
                        <Button size="small" onClick={() => setIsReplacementModalVisible(true)}>
                            Select Replacement
                        </Button>
                    )}
                </Form.Item>
            </Form>
        </Modal>
    );
};
