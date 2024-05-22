import React, { useState } from 'react';

import { Modal, Button } from 'antd';
import { FormOutlined, CodeOutlined } from '@ant-design/icons';

import { useCreateActionPipelineMutation } from '../../../graphql/actionPipeline.generated';
import { YamlEditor } from '../../ingest/source/builder/YamlEditor';

import {
	PremadeAutomations,
	PremadeAutomationCard,
	AutomationsModalHeader,
	AutomationModalFooter,
	YamlButtonsContainer,
	AutomationLogo
} from './components';

import { TextButton } from '../sharedComponents';

import { Configure } from '../Configure';
import { selectableAutomations, getAutomationData } from '../Configure/utils';
import { getYaml } from '../utils';

const SelectPremadeAutomation = ({ setAutomation }: any) => {
	return (
		<PremadeAutomations>
			{selectableAutomations.map((automation: any) => (
				<PremadeAutomationCard
					key={automation.key}
					onClick={() => setAutomation(automation.key)}
					isDisabled={automation.isDisabled}
				>
					{automation.logo && <AutomationLogo src={automation.logo} alt={automation.name} />}
					<h2>{automation.name}</h2>
					<p>{automation.description}</p>
				</PremadeAutomationCard>
			))}
		</PremadeAutomations>
	);
}

export const AutomationCreateModal = ({ isOpen, setIsOpen }: any) => {
	const [createActionPipelineMutation] = useCreateActionPipelineMutation();
	const [automation, setAutomation] = useState();
	const [formData, setFormData] = useState<any>({});
	const [showYaml, setShowYaml] = useState(false);

	// Get the automation info
	const automationData = automation ? getAutomationData(automation) : {} as any;
	const automationType = automation ? automationData?.type : undefined;

	// Transform the recipe
	const baseRecipe = automation ? automationData?.baseRecipe as any : {} as any;

	// Check if the form is disabled
	const isDisabled = !formData?.details?.name || formData?.termsSelected?.length === 0;

	// Close the modal util
	const closeModal = () => {
		setIsOpen(false);
		setShowYaml(false);
		setFormData({});
		setAutomation(undefined);
	};

	// Handle going back to the automation selection
	const goBack = () => {
		setAutomation(undefined);
		setFormData({});
		setShowYaml(false);
	};

	// Handle form submission
	const handleSubmit = () => {
		if (!isDisabled) {
			if (automationType === 'actionPipeline') {
				// Update recipe with terms selected
				// This is a temporary solution until we have a better way to handle this
				baseRecipe.action.config.term_propagation.target_terms = formData.termsSelected || "[]";

				// Run the mutation
				createActionPipelineMutation({
					variables: {
						input: {
							name: formData.details.name,
							description: formData.details.description,
							type: 'term_propagation', // e.g. tag-propagation, term-propagation. Should match the recipe.
							config: {
								recipe: JSON.stringify(baseRecipe),
								version: undefined,
								executorId: 'default',
								debugMode: false,
							},
						},
					},
				});
			}

			closeModal();
		}
	}

	const onYamlChange = (yaml: string) => {
		console.log(yaml);
	}

	return (
		<Modal
			title={automation ? (
				<AutomationsModalHeader>
					{automationData.logo && <AutomationLogo src={automationData.logo} alt={automationData.name} />}
					<div>
						<h2>{automationData.name}</h2>
						<p>{automationData.description}</p>
					</div>
				</AutomationsModalHeader>
			) : (
				<AutomationsModalHeader>
					<div>
						<h2>Create an Automation</h2>
						<p>Select an automation type to begin creating a new automation.</p>
					</div>
				</AutomationsModalHeader>
			)}
			footer={(
				<AutomationModalFooter>
					<div>
						{automation && (
							<YamlButtonsContainer>
								<TextButton type="text" isActive={!showYaml} onClick={() => setShowYaml(false)}>
									<FormOutlined /> Form
								</TextButton>
								<TextButton type="text" isActive={showYaml} onClick={() => setShowYaml(true)}>
									<CodeOutlined /> YAML
								</TextButton>
							</YamlButtonsContainer>
						)}
					</div>
					<div>
						{automation && (<Button onClick={goBack}>Back</Button>)}
						{!automation && (<Button onClick={closeModal}>Cancel</Button>)}
						<Button type="primary" onClick={handleSubmit} disabled={isDisabled}>Save and Run</Button>
					</div>
				</AutomationModalFooter>
			)}
			onCancel={closeModal}
			open={isOpen}
			width={800}
		>
			{!automation && <SelectPremadeAutomation setAutomation={setAutomation} />}
			{automation && !showYaml && <Configure automation={automation} formData={formData} setFormData={setFormData} />}
			{automation && showYaml && (
				<YamlEditor
					initialText={getYaml(automationData, formData) || ''}
					height="450px"
					onChange={onYamlChange}
				/>
			)}
		</Modal>
	)
};