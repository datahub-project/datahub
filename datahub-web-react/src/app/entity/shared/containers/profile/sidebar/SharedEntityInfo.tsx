import React, { useState } from 'react';

import { Typography, Button, Tooltip, message } from 'antd';
import { SyncOutlined, LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { useEntityContext } from '../../../EntityContext';
import { ANTD_GRAY } from '../../../constants';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { sortSharedList } from '../utils';
import analytics, { EventType } from '../../../../../analytics';
import { useShareEntityMutation } from '../../../../../../graphql/share.generated';
import { EntityType, ShareResult } from '../../../../../../types.generated';

export const StyledContainer = styled.div`		
		font-size: 11px;

		&:nth-child(n+3) {
			border-top: 0px !important;
			padding-top: 0px;
		}
`;

export const TitleContainer = styled.div`		
		display: flex;
		margin-bottom: 5px;
		align-items: center;
`;

export const StyledTitle = styled(Typography.Title)`
		text-wrap: balance;
		font-size: 13px !important;
		margin-bottom: 0px !important;

		> span {
			font-weight: normal;
			color: ${ANTD_GRAY[7]};
		}
`;

export const ResyncBytton = styled(Button)`
		height: 24px;
    width: 24px;
    line-height: 0;
    padding: 0.25rem;
		margin-left: 0.75rem;

		> .anticon {
			font-size: 12px;
		}
`;

export const ViewLink = styled(Typography.Link)`
		display: block;
		margin-bottom: 1rem;
`;

interface Props {
	lastShareResults: ShareResult[];
	showMore?: boolean;
}

export const SharedEntityInfo = ({ lastShareResults, showMore = true }: Props) => {
	const { entityData, refetch } = useEntityContext();
	const [shareEntityMutation] = useShareEntityMutation();
	const [shownCount, setShownCount] = useState(showMore ? 1 : 100);
	const [entityLoading, setEntityLoading] = useState<string>();

	// Hide if no share result 
	if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

	// TODO (PRD-944): handle partial successes and have no need to filter these anymore
	// filter results to get only those that have succeeded before
	const filteredResults = lastShareResults.filter(result => !!result.lastSuccess?.time);

	// Sort the list 
	const sortedResults = sortSharedList(filteredResults);

	// Handle Resync
	const handleSubmit = (connectionUrn: string) => {
		setEntityLoading(connectionUrn);

		if (entityData?.urn)
			shareEntityMutation({
				variables: {
					input: {
						entityUrn: entityData.urn,
						connectionUrn,
					}
				}
			})
				.then(({ data, errors }) => {
					message.destroy();
					if (!errors && data?.shareEntity.succeeded) {
						analytics.event({
							type: EventType.SharedEntityEvent,
							entityType: EntityType.DatahubConnection,
							entityUrn: entityData.urn || '',
							connectionUrn,
						});
						message.success({
							content: `Shared entity!`,
							duration: 3,
						});
						refetch();
						setEntityLoading(undefined);
					} else {
						message.error({ content: `Failed to share entity`, duration: 3 });
					}
				})
				.catch((e) => {
					message.destroy();
					message.error({ content: `Failed to share entity!: \n ${e.message || ''}`, duration: 3 });
					setEntityLoading(undefined);
				});
	}


	return (
		<>
			{sortedResults.slice(0, shownCount).map((result) => {
				const lastSuccessTime = result.lastSuccess?.time || 0;
				const name = result.destination.details.name || result.destination.urn;
				return (
					<StyledContainer>
						<TitleContainer>
							<StyledTitle level={5}>
								Shared with <span>{name}</span>
							</StyledTitle>
							<ResyncBytton
								type="text"
								shape="circle"
								onClick={() => handleSubmit(result.destination.urn)}
							>
								{entityLoading === result.destination.urn
									? (
										<Tooltip title="Sharing entity…">
											<LoadingOutlined />
										</Tooltip>
									)
									: (
										<Tooltip title="Sync entity">
											<SyncOutlined />
										</Tooltip>
									)
								}
							</ResyncBytton>
						</TitleContainer>
						last synced on {toLocalDateTimeString(lastSuccessTime)}
					</StyledContainer>
				)
			})}
			{showMore && filteredResults.length > 1 && (
				<ViewLink
					onClick={() => setShownCount(shownCount > 1 ? 1 : filteredResults.length)}
				>
					{shownCount > 1 ? 'View Less' : `View All (${filteredResults.length})`}
				</ViewLink>
			)}
		</>
	);
}