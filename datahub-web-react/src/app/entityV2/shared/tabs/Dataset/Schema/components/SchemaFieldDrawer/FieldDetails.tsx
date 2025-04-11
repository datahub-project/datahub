import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import MarkAsDeprecatedButton from '@src/app/entityV2/shared/components/styled/MarkAsDeprecatedButton';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Deprecation, SubResourceType, UsageQueryResult } from '../../../../../../../../types.generated';
import { useMutationUrn } from '../../../../../../../entity/shared/EntityContext';
import { UpdateDeprecationModal } from '../../../../../EntityDropdown/UpdateDeprecationModal';
import CreateEntityAnnouncementModal from '../../../../../announce/CreateEntityAnnouncementModal';
import { DeprecationIcon } from '../../../../../components/styled/DeprecationIcon';
import { REDESIGN_COLORS } from '../../../../../constants';
import { FieldPopularity } from './FieldPopularity';

const FieldDetailsWrapper = styled.div`
    padding: 16px 12px;
`;

const FieldDetailsContent = styled.div`
    display: flex;
    gap: 10px;
    border-bottom: 1px dashed;
    border-color: rgba(0, 0, 0, 0.3);
    padding-bottom: 16px;
    & > div {
        &:not(:first-child) {
            border-left: 1px dashed;
            border-color: rgba(0, 0, 0, 0.3);
        }
    }
`;

const PopularityContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
    padding: 0px 12px;
`;

const NotesWrapper = styled.div`
    align-items: start;
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 0px 16px;
`;

const DeprecationWrapper = styled.div`
    align-items: start;
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 0px 16px;
`;

const MarkAsDeprecatedButtonContainer = styled.div`
    margin-left: -4px;
`;

const DetailLabel = styled(Typography.Text)`
    color: rgb(55, 64, 102);
    font-size: 12px;
    font-weight: 600;
    line-height: 16px;
`;

const DetailValue = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    opacity: 0.5;
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
    width: max-content;
`;

type FieldDetailsProps = {
    fieldPath: string | null;
    deprecation?: Deprecation | null;
    usageStats?: UsageQueryResult | null;
    refetch?: () => void;
    refetchNotes?: () => void;
};

export const FieldDetails = ({ fieldPath, deprecation, usageStats, refetch, refetchNotes }: FieldDetailsProps) => {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const [isPostModalVisible, setIsPostModalVisible] = useState(false);

    const datasetUrn = useMutationUrn();

    return (
        <FieldDetailsWrapper>
            {isDeprecationModalVisible && (
                <UpdateDeprecationModal
                    urns={[datasetUrn || '']}
                    resourceRefs={[
                        {
                            resourceUrn: datasetUrn,
                            subResource: fieldPath,
                            subResourceType: SubResourceType.DatasetField,
                        },
                    ]}
                    onClose={() => setIsDeprecationModalVisible(false)}
                    refetch={refetch}
                    zIndexOverride={1000}
                />
            )}
            {isPostModalVisible && (
                <CreateEntityAnnouncementModal
                    subResource={fieldPath}
                    urn={datasetUrn}
                    onClose={() => setIsPostModalVisible(false)}
                    onCreate={refetchNotes}
                />
            )}
            <FieldDetailsContent>
                <PopularityContainer>
                    <DetailLabel>Popularity</DetailLabel>
                    <DetailValue>
                        <FieldPopularity
                            isFieldSelected={false}
                            usageStats={usageStats}
                            fieldPath={fieldPath}
                            displayOnDrawer
                        />
                    </DetailValue>
                </PopularityContainer>
                <NotesWrapper>
                    <DetailLabel>Notes</DetailLabel>
                    {isSchemaEditable && (
                        <Button
                            type="text"
                            style={{
                                width: 70,
                                padding: 0,
                                marginTop: -8,
                                color: REDESIGN_COLORS.LINK_GREY,
                            }}
                            onClick={() => {
                                setIsPostModalVisible(true);
                            }}
                        >
                            + Add Note
                        </Button>
                    )}
                </NotesWrapper>
                <DeprecationWrapper>
                    <DetailLabel>Deprecation</DetailLabel>
                    {!deprecation?.deprecated && (
                        <MarkAsDeprecatedButtonContainer>
                            <MarkAsDeprecatedButton onClick={() => setIsDeprecationModalVisible(true)} />
                        </MarkAsDeprecatedButtonContainer>
                    )}
                    {!!deprecation?.deprecated && (
                        <DeprecationIcon
                            urn={datasetUrn}
                            subResource={fieldPath}
                            subResourceType={SubResourceType.DatasetField}
                            deprecation={deprecation}
                            showUndeprecate
                            refetch={refetch}
                            // default zIndex of the popover
                            zIndexOverride={1030}
                        />
                    )}
                </DeprecationWrapper>
            </FieldDetailsContent>
        </FieldDetailsWrapper>
    );
};
