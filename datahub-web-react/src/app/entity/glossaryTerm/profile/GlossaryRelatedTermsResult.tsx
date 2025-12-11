/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import AddRelatedTermsModal from '@app/entity/glossaryTerm/profile/AddRelatedTermsModal';
import RelatedTerm from '@app/entity/glossaryTerm/profile/RelatedTerm';
import { EmptyTab } from '@app/entity/shared/components/styled/EmptyTab';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { Message } from '@app/shared/Message';
import { Button } from '@src/alchemy-components';

import { TermRelationshipType } from '@types';

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
                            {glossaryRelatedTermType}
                        </Typography.Title>
                        {canEditRelatedTerms && (
                            <Button variant="text" onClick={() => setIsShowingAddModal(true)}>
                                <PlusOutlined /> Add Terms
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
