import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useTranslation } from 'react-i18next';
import { TermRelationshipType } from '../../../../types.generated';
import { Message } from '../../../shared/Message';
import { EmptyTab } from '../../shared/components/styled/EmptyTab';
import { ANTD_GRAY } from '../../shared/constants';
import AddRelatedTermsModal from './AddRelatedTermsModal';
import RelatedTerm from './RelatedTerm';
import { translateDisplayNames } from '../../../../utils/translation/translation';

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

const ListContainer = styled.div`
    width: 100%;
`;

const TitleContainer = styled.div`
    align-items: center;
    border-bottom: solid 1px ${ANTD_GRAY[4]};
    display: flex;
    justify-content: space-between;
    padding: 15px 20px;
    margin-bottom: 30px;
`;

const messageStyle = { marginTop: '10%' };

export default function GlossaryRelatedTermsResult({ glossaryRelatedTermType, glossaryRelatedTermResult }: Props) {
    const { t } = useTranslation();
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
                <ListContainer>
                    <TitleContainer>
                        <Typography.Title style={{ margin: '0' }} level={3}>
                            {translateDisplayNames(t, glossaryRelatedTermType)}
                        </Typography.Title>
                        {canEditRelatedTerms && (
                            <Button type="text" onClick={() => setIsShowingAddModal(true)}>
                                <PlusOutlined /> {t('common.addTerm')}
                            </Button>
                        )}
                    </TitleContainer>
                    {glossaryRelatedTermUrns.map((urn) => (
                        <RelatedTerm
                            key={urn}
                            urn={urn}
                            relationshipType={relationshipType}
                            isEditable={canEditRelatedTerms}
                        />
                    ))}
                    {glossaryRelatedTermUrns.length === 0 && (
                        <EmptyTab tab={glossaryRelatedTermType.toLocaleLowerCase()} />
                    )}
                </ListContainer>
            )}
            {isShowingAddModal && (
                <AddRelatedTermsModal onClose={() => setIsShowingAddModal(false)} relationshipType={relationshipType} />
            )}
        </>
    );
}
