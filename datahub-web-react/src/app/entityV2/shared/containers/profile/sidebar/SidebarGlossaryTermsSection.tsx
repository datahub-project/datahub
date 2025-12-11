import AddRoundedIcon from '@mui/icons-material/AddRounded';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useBaseEntity, useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useEntityDataExtractor } from '@app/entityV2/shared/containers/profile/sidebar/hooks/useEntityDataExtractor';
import { getProposedItemsByType } from '@app/entityV2/shared/utils';
import { ENTITY_PROFILE_GLOSSARY_TERMS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import ConstraintGroup from '@app/shared/constraints/ConstraintGroup';
import { findTopLevelProposals } from '@app/shared/tags/utils/proposalUtils';
import AddTagTerm from '@app/sharedV2/tags/AddTagTerm';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { ActionRequestType, EntityType } from '@types';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

interface Props {
    readOnly?: boolean;
    properties?: any;
}

export const SidebarGlossaryTermsSection = ({ readOnly, properties }: Props) => {
    const { entityType, entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();

    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState<EntityType | undefined>(undefined);

    // Extract glossary terms using custom hook (for Business Attributes support)
    const { data: glossaryTerms, isEmpty: areTermsEmpty } = useEntityDataExtractor({
        customPath: properties?.customTermPath,
        defaultPath: 'glossaryTerms',
        arrayProperty: 'terms',
    });

    // Preserve Acryl proposals functionality
    const proposedTerms = findTopLevelProposals(
        getProposedItemsByType(entityData?.proposals || [], ActionRequestType.TermAssociation) || [],
    );

    const areTermsEmptyWithProposals = areTermsEmpty && !proposedTerms?.length;

    const canEditGlossaryTerms = !!entityData?.privileges?.canEditGlossaryTerms;
    const canProposeGlossaryTerms = !!entityData?.privileges?.canProposeGlossaryTerms;

    return (
        <div id={ENTITY_PROFILE_GLOSSARY_TERMS_ID}>
            <SidebarSection
                title="Terms"
                content={
                    <Content>
                        <ConstraintGroup constraints={baseEntity?.dataset?.constraints || []} />
                        {!areTermsEmptyWithProposals ? (
                            <TagTermGroup
                                editableGlossaryTerms={glossaryTerms}
                                canAddTerm
                                canRemove
                                entityUrn={mutationUrn}
                                entityType={entityType}
                                refetch={refetch}
                                readOnly={readOnly}
                                fontSize={12}
                                proposedGlossaryTerms={proposedTerms}
                                showAddButton={false}
                            />
                        ) : (
                            <EmptySectionText message={EMPTY_MESSAGES.terms.title} />
                        )}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={<AddRoundedIcon />}
                        onClick={(event) => {
                            setShowAddModal(true);
                            setAddModalType(EntityType.GlossaryTerm);
                            event.stopPropagation();
                        }}
                        actionPrivilege={canEditGlossaryTerms || canProposeGlossaryTerms}
                        dataTestId="add-terms-button"
                    />
                }
            />
            <AddTagTerm
                entityUrn={mutationUrn}
                entityType={entityType}
                showAddModal={showAddModal}
                setShowAddModal={setShowAddModal}
                addModalType={addModalType}
                refetch={refetch}
                canAddTerm={canEditGlossaryTerms}
                canProposeTerm={canProposeGlossaryTerms}
            />
        </div>
    );
};
