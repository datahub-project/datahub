import { Typography } from 'antd';
import i18next from 'i18next';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import AddRelatedTermsModal from '@app/entityV2/glossaryTerm/profile/AddRelatedTermsModal';
import RelatedTerm from '@app/entityV2/glossaryTerm/profile/RelatedTerm';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import { Message } from '@app/shared/Message';
import { CustomIcon } from '@app/sharedV2/icons/customIcons/CustomIcon';
import addTerm from '@app/sharedV2/icons/customIcons/add-term.svg';
import { Button } from '@src/alchemy-components';

import { TermRelationshipType } from '@types';

// Enum keys map to GraphQL relationship fields and the values are used as comparison keys, so both stay
// stable (English) and must not be translated. User-facing labels are resolved via getRelatedTermTypeLabel.
export enum RelatedTermTypes {
    hasRelatedTerms = 'Contains',
    isRelatedTerms = 'Inherits',
    containedBy = 'Contained by',
    isAChildren = 'Inherited by',
}

const RELATED_TERM_TYPE_LABELS: Record<RelatedTermTypes, () => string> = {
    [RelatedTermTypes.hasRelatedTerms]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.hasRelatedTerms'),
    [RelatedTermTypes.isRelatedTerms]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.isRelatedTerms'),
    [RelatedTermTypes.containedBy]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.containedBy'),
    [RelatedTermTypes.isAChildren]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.isAChildren'),
};

export function getRelatedTermTypeLabel(type: string): string {
    return RELATED_TERM_TYPE_LABELS[type as RelatedTermTypes]?.() ?? type;
}

type Props = {
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
    const { t } = useTranslation('entity.types');
    const { t: tf } = useTranslation('common.feedback');
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
                <Message type="loading" content={tf('loading')} style={messageStyle} />
            ) : (
                <ListWrapper>
                    <TitleContainer>
                        <Typography.Title style={{ margin: '0' }} level={3}>
                            {getRelatedTermTypeLabel(glossaryRelatedTermType)}
                        </Typography.Title>
                        {canEditRelatedTerms && (
                            <Button
                                variant="text"
                                onClick={() => setIsShowingAddModal(true)}
                                data-testid="add-related-term-button"
                            >
                                <CustomIcon iconSvg={addTerm} /> {t('glossaryTerm.addTerms')}
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
