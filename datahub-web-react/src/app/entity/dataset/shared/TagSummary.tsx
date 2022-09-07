import React from 'react';
import styled from 'styled-components';
import { useGetTagQuery } from '../../../../graphql/tag.generated';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { StyledTag } from '../../shared/components/styled/StyledTag';

const TagLink = styled.span`
    display: inline-block;
`;

type Props = {
    urn: string;
};

export const TagSummary = ({ urn }: Props) => {
    const { data } = useGetTagQuery({ variables: { urn } });
    return (
        <HoverEntityTooltip>
            <TagLink key={data?.tag?.urn}>
                <StyledTag
                    style={{ cursor: 'pointer' }}
                    $colorHash={data?.tag?.urn}
                    $color={data?.tag?.properties?.colorHex}
                    closable={false}
                >
                    {data?.tag?.properties?.name}
                </StyledTag>
            </TagLink>
        </HoverEntityTooltip>
    );
};
