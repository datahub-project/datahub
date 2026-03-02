import { colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { EDITING_DOCUMENTATION_URL_PARAM } from '@app/entityV2/shared/constants';
import { useGlossaryActiveTabPath } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';

const TermWrapper = styled.div<{ $isSelected: boolean; $depth: number }>`
    padding-left: calc(${(props) => (props.$depth ? props.$depth * 24 + 12 : 18)}px);
    background-color: ${(props) => props.$isSelected && props.theme.colors.bgActive}};
    display: flex;

    &:hover {
        background-color: ${colors.gray[100]};
        a,
        span {
            color: ${(props) => props.theme.colors.textBrand};
        }
    }
`;

const nameStyles = `
    padding: 16px 0;
    display: inline-block;
    height: 100%;
    width: 100%;
    font-size: 14px;
    font-weight: 400;
    line-height: normal;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;

    &:hover {
        color: ${(props) => props.theme.colors.textActive};
        opacity: 1;
    }
`;

interface TermLinkProps {
    $isSelected: boolean;
    $areChildrenVisible?: boolean;
    $isChildNode?: boolean;
    $entityType?: string;
}

export const TermLink = styled(Link)<TermLinkProps>`
    ${nameStyles}

    ${(props) => props.$isChildNode && `opacity: 1;`}
    ${(props) => props.$areChildrenVisible && `color: ${props.theme.colors.textActive}; font-weight: 500; opacity: 1;`}
    ${(props) => props.$isSelected && `color: ${props.theme.colors.textActive}}; font-weight: 700; opacity: 1;`}
`;

export const NameWrapper = styled.span<{ showSelectStyles?: boolean }>`
    ${nameStyles}

    &:hover {
        ${(props) =>
            props.showSelectStyles &&
            `
        cursor: pointer;
        `}
    }
`;

interface Props {
    term: ChildGlossaryTermFragment;
    isSelecting?: boolean;
    selectTerm?: (urn: string, displayName: string) => void;
    includeActiveTabPath?: boolean;
    areChildrenVisible?: boolean;
    depth: number;
}

function TermItem(props: Props) {
    const { term, isSelecting, selectTerm, includeActiveTabPath, areChildrenVisible } = props;

    const { entityData } = useGlossaryEntityData();
    const entityRegistry = useEntityRegistry();
    const activeTabPath = useGlossaryActiveTabPath();

    function handleSelectTerm() {
        if (selectTerm) {
            const displayName = entityRegistry.getDisplayName(term.type, term);
            selectTerm(term.urn, displayName);
        }
    }

    const isOnEntityPage = entityData && entityData.urn === term.urn;

    const isActivelyEditing = activeTabPath.includes(EDITING_DOCUMENTATION_URL_PARAM);

    return (
        <TermWrapper $isSelected={entityData?.urn === term.urn} $depth={props.depth}>
            {!isSelecting && (
                <TermLink
                    to={`${entityRegistry.getEntityUrl(term.type, term.urn)}${
                        includeActiveTabPath && !isActivelyEditing ? `/${activeTabPath}` : ''
                    }`}
                    $isSelected={entityData?.urn === term.urn}
                    $areChildrenVisible={areChildrenVisible}
                    $entityType={term.type}
                >
                    {entityRegistry.getDisplayName(term.type, isOnEntityPage ? entityData : term)}
                </TermLink>
            )}
            {isSelecting && (
                <NameWrapper showSelectStyles={!!selectTerm} onClick={handleSelectTerm}>
                    {entityRegistry.getDisplayName(term.type, isOnEntityPage ? entityData : term)}
                </NameWrapper>
            )}
        </TermWrapper>
    );
}

export default TermItem;
