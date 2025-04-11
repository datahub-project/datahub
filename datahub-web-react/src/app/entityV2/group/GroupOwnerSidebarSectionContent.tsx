import React, { useState } from 'react';
import { Typography } from 'antd';
import { EntityType, Ownership } from '../../../types.generated';
import { ExpandedOwner } from '../shared/components/styled/ExpandedOwner/ExpandedOwner';
import { EditOwnersModal } from '../shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { ShowMoreSection } from '../shared/sidebarSection/ShowMoreSection';
import { TagsSection } from '../shared/SidebarStyledComponents';

type Props = {
    ownership: Ownership;
    refetch: () => Promise<any>;
    urn: string;
    showAddOwnerModal: boolean;
    setShowAddOwnerModal: (showAddOwnerModal) => void;
};

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

export default function GroupOwnerSidebarSectionContent({
    urn,
    ownership,
    refetch,
    showAddOwnerModal,
    setShowAddOwnerModal,
}: Props) {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const ownershipCount = ownership?.owners?.length || 0;
    const ownersEmpty = !ownership?.owners?.length;

    return (
        <>
            <TagsSection>
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">No group owners added yet.</Typography.Paragraph>
                )}
                {ownership &&
                    ownership?.owners?.map(
                        (owner, index) =>
                            index < entityCount && <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />,
                    )}
            </TagsSection>
            {ownershipCount > entityCount && (
                <ShowMoreSection
                    totalCount={ownershipCount}
                    entityCount={entityCount}
                    setEntityCount={setEntityCount}
                    showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                />
            )}
            {showAddOwnerModal && (
                <EditOwnersModal
                    urns={[urn]}
                    hideOwnerType
                    entityType={EntityType.CorpGroup}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowAddOwnerModal(false);
                    }}
                />
            )}
        </>
    );
}
