import React, { useState } from 'react';

import { List, Typography, Button } from 'antd';

import EntityFormModal from '../../entity/shared/entityForm/EntityFormModal';
import { FormType } from '../../../types.generated';
import { pluralize } from '../../shared/textUtil';

type Props = {
	request: any;
	refetch: () => void;
};

export const RequestItem = ({ request, refetch }: Props) => {
	const [modalOpen, setModalOpen] = useState(false);
	const { form, numEntitiesToComplete } = request;
	const { type, name } = form.info;

	// List of Owners
	const owners = form?.ownership?.owners;
	const isVerificationForm = type === FormType.Verification;

	// Messaging
	let message = isVerificationForm ? `New Verification Request` : `New Documention Request`;
	if (owners && owners.length > 0) {
		const ownerName = owners[0].owner.info.displayName;
		if (ownerName) message = isVerificationForm
			? `New Verification Request from ${ownerName}`
			: `New Documention Request from ${ownerName}`;
	}

	// Close modal & refetch
	const closeModal = () => {
		refetch();
		setModalOpen(false);
	}

	return (
		<>
			<List.Item key={form.urn}>
				<Typography.Text>
					<strong>{message}</strong> <br />
                    Please complete {name} for {numEntitiesToComplete} {pluralize(numEntitiesToComplete, 'asset')}
				</Typography.Text>
				<Button onClick={() => setModalOpen(true)}>
					Open in Documentation Center
				</Button>
			</List.Item>
			<EntityFormModal
				selectedFormUrn={form.urn}
				isFormVisible={modalOpen}
				hideFormModal={closeModal}
			/>
		</>
	);
}
