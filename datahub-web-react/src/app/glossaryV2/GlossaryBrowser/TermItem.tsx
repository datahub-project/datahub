import React from 'react';
import styled from 'styled-components/macro';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { ChildGlossaryTermFragment } from '../../../graphql/glossaryNode.generated';
import { useGlossaryEntityData } from '../../entityV2/shared/GlossaryEntityContext';
import { useGlossaryActiveTabPath } from '../../entityV2/shared/containers/profile/utils';

const TermWrapper = styled.div`
    font-weight: normal;
    line-height: normal;
    margin-left: 2px;

    &:not(:first-child) {
        padding-top: 11px;
    }
`;

const nameStyles = `
    display: inline-block;
    height: 100%;
    width: 100%;
    font-size: 10px;
    font-weight: 400;
    line-height: normal;
    color: ${REDESIGN_COLORS.SUBTITLE};
    opacity: 0.5;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;

    &:hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        opacity: 1;
    }
`;

interface TermLinkProps {
    isSelected: boolean;
    areChildrenVisible?: boolean;
    isChildNode?: boolean;
    entityType?: string;
}

export const TermLink = styled(Link)<TermLinkProps>`
    ${nameStyles}

    ${(props) => props.isChildNode && `opacity: 1;`}
    ${(props) => props.areChildrenVisible && `color: ${REDESIGN_COLORS.TITLE_PURPLE}; font-weight: 400; opacity: 1;`}
    ${(props) => props.isSelected && `color: ${REDESIGN_COLORS.TITLE_PURPLE}; font-weight: 700; opacity: 1;`}
`;

export const NameWrapper = styled.span<{ showSelectStyles?: boolean }>`
    ${nameStyles}

    &:hover {
        ${(props) =>
            props.showSelectStyles &&
            `
        background-color: ${ANTD_GRAY[3]};
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

    return (
        <TermWrapper>
            {!isSelecting && (
                <TermLink
                    to={`${entityRegistry.getEntityUrl(term.type, term.urn)}${
                        includeActiveTabPath ? `/${activeTabPath}` : ''
                    }`}
                    isSelected={entityData?.urn === term.urn}
                    areChildrenVisible={areChildrenVisible}
                    entityType={term.type}
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
