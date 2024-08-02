import React, { useState } from 'react';
import styled from 'styled-components';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import TagTermGroup from '../../../../../sharedV2/tags/TagTermGroup';
import { useBaseEntity, useEntityData, useMutationUrn, useRefetch } from '../../../../../entity/shared/EntityContext';
import { findTopLevelProposals } from '../../../../../shared/tags/utils/proposalUtils';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { ENTITY_PROFILE_GLOSSARY_TERMS_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import ConstraintGroup from '../../../../../shared/constraints/ConstraintGroup';
import { SidebarSection } from './SidebarSection';
import { EntityType } from '../../../../../../types.generated';
import EmptySectionText from './EmptySectionText';
import { EMPTY_MESSAGES } from '../../../constants';
import SectionActionButton from './SectionActionButton';
import AddTagTerm from '../../../../../sharedV2/tags/AddTagTerm';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

interface Props {
    readOnly?: boolean;
}

export const SidebarGlossaryTermsSection = ({ readOnly }: Props) => {
    const { entityType, entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();

    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState<EntityType | undefined>(undefined);

    const proposedTerms = findTopLevelProposals(entityData?.termProposals || []);

    const areTermsEmpty = !entityData?.glossaryTerms?.terms?.length && !proposedTerms?.length;

    const canEditGlossaryTerms = !!entityData?.privileges?.canEditGlossaryTerms;
    const canProposeGlossaryTerms = !!entityData?.privileges?.canProposeGlossaryTerms;

    return (
        <div id={ENTITY_PROFILE_GLOSSARY_TERMS_ID}>
            <SidebarSection
                title="Terms"
                content={
                    <Content>
                        <ConstraintGroup constraints={baseEntity?.dataset?.constraints || []} />
                        {!areTermsEmpty ? (
                            <TagTermGroup
                                editableGlossaryTerms={entityData?.glossaryTerms}
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
            />
        </div>
    );
};
