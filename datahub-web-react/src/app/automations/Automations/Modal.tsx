import React, { useState } from 'react';

import { Modal, Button } from 'antd';
import { FormOutlined, CodeOutlined } from '@ant-design/icons';

import {
	useCreateActionPipelineMutation,
	useUpsertActionPipelineMutation
} from '../../../graphql/actionPipeline.generated';
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

	// Get the automation info
	const automationData = getAutomationData(automation, data?.definition?.type) || {} as any;
	const automationType = automationData ? automationData?.type : undefined;

	// Transform the recipe
	const baseRecipe = automationData ? automationData?.baseRecipe as any : {} as any;

	// Check if the form is disabled
	const isDisabled = false;

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
			if (automationType === 'actionPipeline') {
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
			if (automationType === 'actionPipeline') {
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

		const { name, description, definition } = data;
		const recipe = definition?.config?.recipe ? JSON.parse(definition.config.recipe) : {};

		return {
			...automationData,
			steps: automationData.steps || [], // failsafe
			name,
			description,
			baseRecipe: recipe,
		}
	}

	const isCreate = type === 'CREATE';
	const configureInfo = isCreate && automation ? automationData : mergeDataIfEdit();

	// Conditional form details
	const formInfo = {
		modalTitle: isCreate ? 'Create an Automation' : 'Edit Automation',
		modalDescription: isCreate ? 'Select an automation type to begin creating a new automation.' : configureInfo?.name,
		submitContent: isCreate ? 'Save and Run' : 'Save',
		submitFn: isCreate ? handleCreate : handleUpdate,
	}

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
					<div>
						<h2>{formInfo.modalTitle}</h2>
						<p>{formInfo.modalDescription}</p>
					</div>
				</AutomationsModalHeader>
			)}
			footer={(
				<AutomationModalFooter>
					<div>
						{showForm && (
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
						{isCreate && showForm && (<Button onClick={goBack}>Back</Button>)}
						{!showForm && (<Button onClick={closeModal}>Cancel</Button>)}
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
					initData={data}
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