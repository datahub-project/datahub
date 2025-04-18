import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useBaseEntity, useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { getNestedValue } from '@app/entity/shared/containers/profile/utils';
import {
    ENTITY_PROFILE_GLOSSARY_TERMS_ID,
    ENTITY_PROFILE_TAGS_ID,
} from '@app/onboarding/config/EntityProfileOnboardingConfig';
import ConstraintGroup from '@app/shared/constraints/ConstraintGroup';
import TagTermGroup from '@app/shared/tags/TagTermGroup';
import { findTopLevelProposals } from '@app/shared/tags/utils/proposalUtils';
import { getProposedItemsByType } from '@src/app/entityV2/shared/utils';
import { ActionRequestType } from '@src/types.generated';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const StyledDivider = styled(Divider)`
    margin: 16px 0;
`;

interface Props {
    properties?: any;
    readOnly?: boolean;
}

export const SidebarTagsSection = ({ properties, readOnly }: Props) => {
    const canAddTag = properties?.hasTags;
    const canAddTerm = properties?.hasTerms;

    const mutationUrn = useMutationUrn();

    const { entityType, entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const refetch = useRefetch();

    return (
        <div>
            <span id={ENTITY_PROFILE_TAGS_ID}>
                <SidebarHeader title="Tags" />
                <TagTermGroup
                    editableTags={
                        properties?.customTagPath
                            ? getNestedValue(entityData, properties?.customTagPath)
                            : entityData?.globalTags
                    }
                    canAddTag={canAddTag}
                    canRemove
                    showEmptyMessage
                    entityUrn={mutationUrn}
                    entityType={entityType}
                    refetch={refetch}
                    readOnly={readOnly}
                    fontSize={12}
                    proposedTags={findTopLevelProposals(
                        getProposedItemsByType(entityData?.proposals || [], ActionRequestType.TagAssociation) || [],
                    )}
                />
            </span>
            <StyledDivider />
            <span id={ENTITY_PROFILE_GLOSSARY_TERMS_ID}>
                <SidebarHeader title="Glossary Terms" />
                <ConstraintGroup constraints={baseEntity?.dataset?.constraints || []} />
                <TagTermGroup
                    editableGlossaryTerms={
                        properties?.customTermPath
                            ? getNestedValue(entityData, properties?.customTermPath)
                            : entityData?.glossaryTerms
                    }
                    canAddTerm={canAddTerm}
                    canRemove
                    showEmptyMessage
                    entityUrn={mutationUrn}
                    entityType={entityType}
                    refetch={refetch}
                    readOnly={readOnly}
                    fontSize={12}
                    proposedGlossaryTerms={findTopLevelProposals(entityData?.termProposals || [])}
                />
            </span>
        </div>
    );
};
