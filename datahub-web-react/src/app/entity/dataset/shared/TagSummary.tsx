import React from 'react';
import styled from 'styled-components';
import { useGetTagQuery } from '../../../../graphql/tag.generated';
import { EntityType, Tag } from '../../../../types.generated';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { StyledTag } from '../../shared/components/styled/StyledTag';

const TagLink = styled.span`
    display: inline-block;
`;

type Props = {
    urn: string;
    mode?: 'text' | 'default';
};

export const TagSummary = ({ urn, mode = 'text' }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { data } = useGetTagQuery({ variables: { urn } });
    const tagName = data ? entityRegistry.getDisplayName(EntityType.Tag, data?.tag) : null;

    if (mode === 'text') return <>{tagName}</>;

    return (
        <>
            {data && (
                <HoverEntityTooltip entity={data?.tag as Tag}>
                    <TagLink key={data?.tag?.urn}>
                        <StyledTag
                            style={{ cursor: 'pointer' }}
                            $colorHash={data?.tag?.urn}
                            $color={data?.tag?.properties?.colorHex}
                            closable={false}
                        >
                            {tagName}
                        </StyledTag>
                    </TagLink>
                </HoverEntityTooltip>
            )}
        </>
    );
};
