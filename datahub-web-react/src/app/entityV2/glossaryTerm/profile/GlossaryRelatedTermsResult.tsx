import { Typography } from 'antd';
import React, { useState } from 'react';
import { Button } from '@src/alchemy-components';
import styled from 'styled-components/macro';
import { TermRelationshipType } from '../../../../types.generated';
import { Message } from '../../../shared/Message';
import { EmptyTab } from '../../shared/components/styled/EmptyTab';
import AddRelatedTermsModal from './AddRelatedTermsModal';
import RelatedTerm from './RelatedTerm';
import { CustomIcon } from '../../../sharedV2/icons/customIcons/CustomIcon';
import addTerm from '../../../sharedV2/icons/customIcons/add-term.svg';

export enum RelatedTermTypes {
    hasRelatedTerms = 'Contains',
    isRelatedTerms = 'Inherits',
    containedBy = 'Contained by',
    isAChildren = 'Inherited by',
}

export type Props = {
    glossaryRelatedTermType: string;
    glossaryRelatedTermResult: Array<any>;
};

const ListWrapper = styled.div`
    display: flex;
    padding: 0 16px;
    flex-direction: column;
    width: 100%;
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const TitleContainer = styled.div`
    align-items: center;
    display: flex;
    justify-content: space-between;
    padding: 10px 20px;
    margin-bottom: 10px;
`;

const messageStyle = { marginTop: '10%' };

export default function GlossaryRelatedTermsResult({ glossaryRelatedTermType, glossaryRelatedTermResult }: Props) {
    const [isShowingAddModal, setIsShowingAddModal] = useState(false);
    const glossaryRelatedTermUrns: Array<string> = [];
    glossaryRelatedTermResult.forEach((item: any) => {
        glossaryRelatedTermUrns.push(item?.entity?.urn);
    });
    const contentLoading = false;
    const relationshipType =
        glossaryRelatedTermType === RelatedTermTypes.hasRelatedTerms ||
        glossaryRelatedTermType === RelatedTermTypes.containedBy
            ? TermRelationshipType.HasA
            : TermRelationshipType.IsA;
    const canEditRelatedTerms =
        glossaryRelatedTermType === RelatedTermTypes.isRelatedTerms ||
        glossaryRelatedTermType === RelatedTermTypes.hasRelatedTerms;

    return (
        <>
            {contentLoading ? (
                <Message type="loading" content="Loading..." style={messageStyle} />
            ) : (
                <ListWrapper>
                    <TitleContainer>
                        <Typography.Title style={{ margin: '0' }} level={3}>
                            {glossaryRelatedTermType}
                        </Typography.Title>
                        {canEditRelatedTerms && (
                            <Button variant="text" onClick={() => setIsShowingAddModal(true)}>
                                <CustomIcon iconSvg={addTerm} /> Add Terms
                            </Button>
                        )}
                    </TitleContainer>
                    <ListContainer>
                        {glossaryRelatedTermUrns.map((urn) => (
                            <RelatedTerm
                                key={urn}
                                urn={urn}
                                relationshipType={relationshipType}
                                isEditable={canEditRelatedTerms}
                            />
                        ))}
                    </ListContainer>
                    {glossaryRelatedTermUrns.length === 0 && (
                        <EmptyTab tab={glossaryRelatedTermType.toLocaleLowerCase()} />
                    )}
                </ListWrapper>
            )}
            {isShowingAddModal && (
                <AddRelatedTermsModal onClose={() => setIsShowingAddModal(false)} relationshipType={relationshipType} />
            )}
        </>
    );
}
