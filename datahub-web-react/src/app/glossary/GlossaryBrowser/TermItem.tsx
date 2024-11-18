import React from 'react';
import styled from 'styled-components/macro';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { ChildGlossaryTermFragment } from '../../../graphql/glossaryNode.generated';
import { useGlossaryEntityData } from '../../entity/shared/GlossaryEntityContext';
import { useGlossaryActiveTabPath } from '../../entity/shared/containers/profile/utils';

const TermWrapper = styled.div`
    font-weight: normal;
    margin-bottom: 4px;
`;

const nameStyles = `
    color: #262626;
    display: inline-flex;
    height: 100%;
    padding: 3px 4px;
    width: 100%;
    align-items: center;
`;

export const TermLink = styled(Link)<{ $isSelected }>`
    ${nameStyles}

    ${(props) => props.$isSelected && `background-color: #F0FFFB;`}

    &:hover {
        ${(props) => !props.$isSelected && `background-color: ${ANTD_GRAY[3]};`}
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
    term: ChildGlossaryTermFragment;
    isSelecting?: boolean;
    selectTerm?: (urn: string, displayName: string) => void;
    includeActiveTabPath?: boolean;
    termUrnToHide?: string;
}

function TermItem(props: Props) {
    const { term, isSelecting, selectTerm, includeActiveTabPath, termUrnToHide } = props;
    const shouldHideTerm = termUrnToHide === term.urn;

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

    if (shouldHideTerm) return null;
    return (
        <TermWrapper>
            {!isSelecting && (
                <TermLink
                    to={`${entityRegistry.getEntityUrl(term.type, term.urn)}${
                        includeActiveTabPath ? `/${activeTabPath}` : ''
                    }`}
                    $isSelected={entityData?.urn === term.urn}
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
