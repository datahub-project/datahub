import React, { useState } from 'react';

import { Modal, Button } from 'antd';
import { FormOutlined, CodeOutlined } from '@ant-design/icons';

import {
	useCreateActionPipelineMutation,
	useUpsertActionPipelineMutation
} from '../../../graphql/actionPipeline.generated';
import { YamlEditor } from '../../ingest/source/builder/YamlEditor';

import { TextButton } from '../sharedComponents';

import { Configure } from '../Configure';
import { selectableAutomations, getAutomationData } from '../Configure/utils';
import { AutomationTypes, parseJSON, getYaml } from '../utils';

import {
	PremadeAutomations,
	PremadeAutomationCard,
	AutomationsModalHeader,
	AutomationModalFooter,
	YamlButtonsContainer,
	AutomationLogo
} from './components';

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

type AutomationModalProps = {
	isOpen: boolean;
	setIsOpen: (isOpen: boolean) => void;
	type?: 'CREATE' | 'EDIT';
	data?: any;
};

export const AutomationModal = ({ isOpen, setIsOpen, type = 'CREATE', data }: AutomationModalProps) => {
	const [createActionPipelineMutation] = useCreateActionPipelineMutation();
	const [upsertActionPipelineMutation] = useUpsertActionPipelineMutation();

	const [automation, setAutomation] = useState();
	const [formData, setFormData] = useState<any>({});
	const [showYaml, setShowYaml] = useState(false);

	// Get the definition if it exists
	const definition = parseJSON(data?.definition);

	// Get the automation info
	const automationData = getAutomationData(automation, definition.action?.type) || {} as any;
	const automationType = automationData ? automationData?.type : undefined;

	// Transform the recipe
	const baseRecipe = automationData ? automationData?.baseRecipe as any : {} as any;

	// Check if the form is disabled
	const isDisabled = true;

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

	// Handle form create submission
	const handleCreate = () => {
		if (!isDisabled) {

			// Create action pipeline
			if (automationType === AutomationTypes.ACTION) {
				// Update recipe with terms selected
				// This is a temporary solution until we have a better way to handle this
				baseRecipe.action.config.term_propagation.target_terms = formData.termsSelected || "[]";
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

	// Handle form update submission
	const handleUpdate = () => {
		if (!isDisabled) {

			// Update action pipeline
			if (automationType === AutomationTypes.ACTION) {
				upsertActionPipelineMutation({
					variables: {
						urn: data.urn,
						input: {
							name: formData.details.name,
							description: formData.details.description,
							type: data.type, // TODO: make this mutable?
							config: data.definition, // TODO: make this mutable
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

	const mergeDataIfEdit = () => {
		if (!data) return {};
		const { name, description, category } = data;

		return {
			...automationData,
			steps: automationData.steps || [], // failsafe
			name,
			description,
			category,
			baseRecipe,
			definition,
		}
	}

	const isCreate = type === 'CREATE';
	const configureInfo = isCreate && automation ? automationData : mergeDataIfEdit();

	// Conditional form details
	const formInfo = {
		modalTitle: isCreate ? 'Create an Automation' : 'Edit Automation',
		modalDescription: isCreate
			? 'Select an automation type to begin creating a new automation.'
			: 'Editing this automation will create a new version. You can rollback to previous versions.',
		submitContent: 'Save', // generic, gets updated based on recipe items
		submitFn: isCreate ? handleCreate : handleUpdate,
	}

	if (automationType === AutomationTypes.ACTION) formInfo.submitContent = 'Save and Run';
	if (automationType === AutomationTypes.TEST) formInfo.submitContent = 'Save and Schedule';

	// Form states
	const showPreselect = isCreate && !automation;
	const showForm = isCreate ? automation && !showYaml : configureInfo && !showYaml;

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
					{configureInfo.logo && <AutomationLogo src={configureInfo.logo} alt={configureInfo.name} />}
					<div>
						<h2>{formInfo.modalTitle}</h2>
						<p>{formInfo.modalDescription}</p>
					</div>
				</AutomationsModalHeader>
			)}
			footer={(
				<AutomationModalFooter>
					<div>
						{(showForm || showYaml) && (
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
						{isCreate && (showForm || showYaml) && (<Button onClick={goBack}>Back</Button>)}
						<Button onClick={closeModal}>Cancel</Button>
						<Button type="primary" onClick={formInfo.submitFn} disabled={isDisabled}>
							{formInfo.submitContent}
						</Button>
					</div>
				</AutomationModalFooter>
			)}
			onCancel={closeModal}
			open={isOpen}
			width={800}
		>
			{showPreselect && (
				<SelectPremadeAutomation
					setAutomation={setAutomation}
				/>
			)}
			{showForm && (
				<Configure
					automation={configureInfo}
					formData={formData}
					setFormData={setFormData}
				/>
			)}
			{showYaml && (
				<YamlEditor
					initialText={getYaml(configureInfo) || ''}
					height="450px"
					onChange={onYamlChange}
					isDisabled
				/>
			)}
		</Modal>
	)
};