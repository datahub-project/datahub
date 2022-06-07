import React from 'react';
import styled from 'styled-components/macro';
import { Link } from 'react-router-dom';
import { useEntityData } from '../../entity/shared/EntityContext';
import { GlossaryTerm } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';

const TermWrapper = styled.div`
    font-weight: normal;
    margin-bottom: 4px;
`;

const nameStyles = `
    color: #262626;
    display: inline-block;
    height: 100%;
    padding: 3px 4px;
    width: 100%;
`;

export const TermLink = styled(Link)<{ isSelected }>`
    ${nameStyles}

    ${(props) => props.isSelected && `background-color: #F0FFFB;`}

    &:hover {
        ${(props) => !props.isSelected && `background-color: ${ANTD_GRAY[3]};`}
        color: #262626;
    }
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
    term: GlossaryTerm;
    isSelecting?: boolean;
    selectTerm?: (urn: string, displayName: string) => void;
}

function TermItem(props: Props) {
    const { term, isSelecting, selectTerm } = props;

    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

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
                    to={`/${entityRegistry.getPathName(term.type)}/${term.urn}`}
                    isSelected={entityData?.urn === term.urn}
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
