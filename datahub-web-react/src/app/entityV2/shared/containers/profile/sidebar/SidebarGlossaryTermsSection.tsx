import React from 'react';
import styled from 'styled-components';
import TagTermGroup from '../../../../../sharedV2/tags/TagTermGroup';
import { useBaseEntity, useEntityData, useMutationUrn, useRefetch } from '../../../EntityContext';
import { findTopLevelProposals } from '../../../../../shared/tags/utils/proposalUtils';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { ENTITY_PROFILE_GLOSSARY_TERMS_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import ConstraintGroup from '../../../../../shared/constraints/ConstraintGroup';
import { SidebarSection } from './SidebarSection';

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

    return (
        <div id={ENTITY_PROFILE_GLOSSARY_TERMS_ID}>
            <SidebarSection
                title="Terms"
                content={
                    <Content>
                        <ConstraintGroup constraints={baseEntity?.dataset?.constraints || []} />
                        <TagTermGroup
                            editableGlossaryTerms={entityData?.glossaryTerms}
                            canAddTerm
                            canRemove
                            showEmptyMessage
                            entityUrn={mutationUrn}
                            entityType={entityType}
                            refetch={refetch}
                            readOnly={readOnly}
                            fontSize={12}
                            proposedGlossaryTerms={findTopLevelProposals(entityData?.termProposals || [])}
                        />
                    </Content>
                }
            />
        </div>
    );
};
