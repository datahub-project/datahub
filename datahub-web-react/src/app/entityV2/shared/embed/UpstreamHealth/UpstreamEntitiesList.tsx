import React from 'react';
import styled from 'styled-components';
import { Dataset } from '@src/types.generated';
import { Divider } from 'antd';
import { EntityLinkList } from '@src/app/homeV2/reference/sections/EntityLinkList';
import { ANTD_GRAY } from '../../constants';

type Props = {
    directEntities: Dataset[];
    indirectEntities: Dataset[];
    loadMoreDirectEntities: () => void;
    loadMoreIndirectEntities: () => void;
    remainingDirectEntities: number;
    remainingIndirectEntities: number;
};

const Container = styled.div`
    padding: 10px;
    background-color: white;
`;

const StyledDivider = styled(Divider)`
    margin: 12px 0;
`;

const SubHeader = styled.div`
    font-weight: 700;
`;

const Content = styled.div`
    padding: 0 3px;
`;

const ShowMoreWrapper = styled.div`
    margin-top: 5px;
    display: flex;
    justify-content: end;
    font-weight: 600;
    color: ${ANTD_GRAY[7]};
    font-size: 12px;
`;

const UpstreamEntitiesList = ({
    directEntities,
    indirectEntities,
    loadMoreDirectEntities,
    loadMoreIndirectEntities,
    remainingDirectEntities,
    remainingIndirectEntities,
}: Props) => {
    return (
        <Container>
            <Content>
                {!!directEntities.length && (
                    <>
                        <SubHeader>Direct Upstreams</SubHeader>
                        <EntityLinkList
                            entities={directEntities}
                            loading={false}
                            showMore={!!remainingDirectEntities}
                            showMoreCount={remainingDirectEntities}
                            onClickMore={loadMoreDirectEntities}
                            showMoreComponent={
                                <ShowMoreWrapper>{`Show ${remainingDirectEntities} more`}</ShowMoreWrapper>
                            }
                            showHealthIcon
                        />
                    </>
                )}

                {!!indirectEntities.length && (
                    <>
                        {!!directEntities.length && <StyledDivider />}
                        <SubHeader>Indirect Upstreams</SubHeader>
                        <EntityLinkList
                            entities={indirectEntities}
                            loading={false}
                            showMore={!!remainingIndirectEntities}
                            showMoreCount={remainingIndirectEntities}
                            onClickMore={loadMoreIndirectEntities}
                            showMoreComponent={
                                <ShowMoreWrapper>{`Show ${remainingIndirectEntities} more`}</ShowMoreWrapper>
                            }
                            showHealthIcon
                        />
                    </>
                )}
            </Content>
        </Container>
    );
};

export default UpstreamEntitiesList;
