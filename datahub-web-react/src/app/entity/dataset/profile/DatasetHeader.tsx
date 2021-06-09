import { Badge, Divider, message, Popover, Space, Tag, Typography } from 'antd';
import { FetchResult } from '@apollo/client';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Dataset } from '../../../../types.generated';
import { UpdateDatasetMutation } from '../../../../graphql/dataset.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import UpdateDescriptionModal from './modal/UpdateDescriptionModal';
import MarkdownViewer from '../../shared/MarkdownViewer';

const DescriptionText = styled(MarkdownViewer)`
    ${(props) => (props.isCompact ? 'max-width: 377px;' : '')};
`;

const AddNewDescription = styled(Tag)`
    cursor: pointer;
`;

const MessageKey = 'AddDatasetDescription';

export type Props = {
    dataset: Dataset;
    updateDescription: (
        description: string | null,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
};

export default function DatasetHeader({
    dataset: { description: originalDesc, ownership, deprecation, platform, editableProperties },
    updateDescription,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);
    const platformName = capitalizeFirstLetter(platform.name);
    const [showAddDescModal, setShowAddDescModal] = useState(false);
    const updatedDesc = editableProperties?.description;

    const onUpdateSubmit = async (desc: string | null) => {
        message.loading({ content: 'Updating...', key: MessageKey });
        await updateDescription(desc);
        message.success({ content: 'Updated!', key: MessageKey, duration: 2 });
        setShowAddDescModal(false);
    };

    return (
        <>
            <Space direction="vertical" size="middle">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Dataset</Typography.Text>
                    <Typography.Text strong>{platformName}</Typography.Text>
                </Space>
                {updatedDesc || originalDesc ? (
                    <>
                        <DescriptionText
                            isCompact={isCompact}
                            source={updatedDesc || originalDesc || ''}
                            editable
                            onEditClicked={() => setShowAddDescModal(true)}
                        />
                        {showAddDescModal && (
                            <UpdateDescriptionModal
                                title="Update description"
                                onClose={() => setShowAddDescModal(false)}
                                onSubmit={onUpdateSubmit}
                                original={originalDesc || ''}
                                description={updatedDesc || ''}
                            />
                        )}
                    </>
                ) : (
                    <>
                        <AddNewDescription color="success" onClick={() => setShowAddDescModal(true)}>
                            + Add Description
                        </AddNewDescription>
                        {showAddDescModal && (
                            <UpdateDescriptionModal
                                title="Add description"
                                onClose={() => setShowAddDescModal(false)}
                                onSubmit={onUpdateSubmit}
                                isAddDesc
                            />
                        )}
                    </>
                )}
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
                <div>
                    {deprecation?.deprecated && (
                        <Popover
                            placement="bottomLeft"
                            content={
                                <>
                                    <Typography.Paragraph>By: {deprecation?.actor}</Typography.Paragraph>
                                    {deprecation.decommissionTime && (
                                        <Typography.Paragraph>
                                            On: {new Date(deprecation?.decommissionTime).toUTCString()}
                                        </Typography.Paragraph>
                                    )}
                                    {deprecation?.note && (
                                        <Typography.Paragraph>{deprecation.note}</Typography.Paragraph>
                                    )}
                                </>
                            }
                            title="Deprecated"
                        >
                            <Badge count="Deprecated" />
                        </Popover>
                    )}
                </div>
            </Space>
        </>
    );
}
