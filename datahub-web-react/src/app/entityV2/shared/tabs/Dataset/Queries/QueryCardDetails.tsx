import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../../constants';
import { toLocalDateString } from '../../../../../shared/time/timeUtils';
import NoMarkdownViewer from '../../../components/styled/StripMarkdownText';
import QueryCardDetailsMenu from './QueryCardDetailsMenu';
import QueryCardEditButton from './QueryCardEditButton';

const Title = styled(Typography.Title)<{ secondary?: boolean }>`
    && {
        margin: 0px;
        padding: 0px;
        color: ${(props) => (props.secondary && ANTD_GRAY[6]) || ANTD_GRAY[9]};
    }
    max-height: 40px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Details = styled.div`
    padding: 0px 20px 0px 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    white-space: nowrap;
`;

const Actions = styled.div`
    flex-wrap: nowrap;
`;

const EditQueryAction = styled.span`
    && {
        margin: 0px;
        padding: 0px;
        margin-left: 4px;
    }
`;

const Description = styled.div`
    margin-bottom: 16px;
    height: 52px;
    overflow: auto;
`;

const MoreButton = styled.span`
    :hover {
        cursor: pointer;
    }
    font-weight: bold;
`;

const Date = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const EmptyText = styled.div`
    && {
        color: ${ANTD_GRAY[6]};
    }
`;

export type Props = {
    urn?: string;
    title?: string;
    description?: string;
    createdAtMs?: number;
    showDelete?: boolean;
    showEdit?: boolean;
    onDeleted?: (urn) => void;
    onClickExpand?: () => void;
    onClickEdit?: () => void;
    index?: number;
};

export default function QueryCardDetails({
    urn,
    title,
    description,
    createdAtMs,
    showDelete,
    showEdit,
    onClickExpand,
    onClickEdit,
    onDeleted,
    index,
}: Props) {
    return (
        <Details>
            <Header>
                <Title secondary={!title} level={5}>
                    {title || 'No title'}
                </Title>
                <Actions>
                    {showEdit && (
                        <EditQueryAction>
                            <QueryCardEditButton onClickEdit={onClickEdit} index={index} />
                        </EditQueryAction>
                    )}
                    {showDelete && urn && (
                        <EditQueryAction>
                            <QueryCardDetailsMenu urn={urn} onDeleted={onDeleted} index={index} />
                        </EditQueryAction>
                    )}
                </Actions>
            </Header>
            <Description>
                {(description && (
                    <NoMarkdownViewer
                        shouldWrap
                        limit={200}
                        readMore={<MoreButton onClick={onClickExpand}>more</MoreButton>}
                    >
                        {description}
                    </NoMarkdownViewer>
                )) || <EmptyText>No description</EmptyText>}
            </Description>
            <Date>
                {(createdAtMs && (
                    <Typography.Text type="secondary">Created on {toLocalDateString(createdAtMs)}</Typography.Text>
                )) ||
                    undefined}
            </Date>
        </Details>
    );
}
