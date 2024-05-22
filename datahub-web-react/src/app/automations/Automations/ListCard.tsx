import React from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';

import { CustomAvatar } from '../../shared/avatar';

import {
	ListCard,
	ListCardHeader,
	ListCardBody,
} from './components';

import { truncateString } from '../utils';

dayjs.extend(localizedFormat);

const PropagationDetails = () => {
	return (
		<>[propgation details]</>
	)
};

const MetadataTestDetails = () => {
	return (
		<>[metadata test details]</>
	)
};

export const AutomationsListCard = ({ automation }: any) => {
	const { type } = automation;

	let contentTitle = 'Details';
	if (type === 'Test') contentTitle = 'Results';

	return (
		<ListCard>
			<ListCardHeader>
				<div className="categoryAndDeployed">
					<h4>{automation.category}</h4>
					<h4>{dayjs(automation.updated).format('L')}</h4>
				</div>
				<div className="titleAndStatus">
					<h2>{automation.name}</h2>
					<div className="status">
						Status
					</div>
				</div>
			</ListCardHeader>
			<ListCardBody>
				<div className="createdBy">
					Created by<CustomAvatar name="John Doe" />xyz
				</div>
				{automation.description && (
					<div className="description">
						<p>{truncateString(automation.description, 125)}</p>
					</div>
				)}
				<div>
					<h3>{contentTitle}</h3>
					{type === 'ActionPipeline' && <PropagationDetails />}
					{type === 'Test' && <MetadataTestDetails />}
				</div>
			</ListCardBody>
		</ListCard>
	);
};
