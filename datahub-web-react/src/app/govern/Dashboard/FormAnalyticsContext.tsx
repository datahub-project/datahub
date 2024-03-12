import React, { useContext, useState, useEffect, ReactNode } from 'react';

import dayjs from 'dayjs';

import { useFormAnalyticsQuery } from '../../../graphql/analytics.generated';
import { sqlQueries } from './charts/queries';

// Define the time series options
const timeSeries = [
	{
		key: 7, // days count
		label: 'Last 7 days',
		tooltip: 'Forms assigned in the last week'
	},
	{
		key: 30,
		label: 'Last 30 days',
		tooltip: 'Forms assigned in the last month'
	},
	{
		key: 90,
		label: 'Last 90 days',
		tooltip: 'Forms assigned in the last quarter'
	},
	{
		key: 365,
		label: 'Last 365 days',
		tooltip: 'Forms assigned in the last year'
	}
];

// Define the context type
export interface FormAnalyticsContextType {
	sql: any;
	integrationServiceOffline: boolean;
	contextLoading: boolean;
	snapshot?: string;
	tabs: {
		selectedTab: string;
		setSelectedTab: (tab: string) => void;
	},
	timeSeries: {
		options: Array<{ key: number, label: string, tooltip: string }>;
		selectedSeries: number;
		setSeries: (series: number) => void;
		getSeriesInfo: () => any;
	},
	byForm: {
		forms: any;
		hasForms: boolean;
		selectedForm?: string;
		setSelectedForm: (value: string) => void;
	},
	byAssignee: {
		assignees: any;
		hasAssignees: boolean;
		selectedAssignee?: string;
		setSelectedAssignee: (value: string) => void;
	},
	byDomain: {
		domains: any;
		hasDomains: boolean;
		selectedDomain?: string;
		setSelectedDomain: (value: string) => void;
	},
	sectionLoadStates: { // these are in order that they appear in the UI
		stats: boolean;
		overallProgress: boolean;
		forms: boolean;
		questions: boolean;
		assignees: boolean;
		domains: boolean;
		setLoadStates: (group: any, key: any, value: boolean) => void;
		resetLoadStates: () => void;
	}
}

// Default values
export const FormAnalyticsContext = React.createContext<FormAnalyticsContextType>({
	sql: {},
	integrationServiceOffline: false,
	contextLoading: true,
	snapshot: undefined,
	tabs: {
		selectedTab: '',
		setSelectedTab: () => null
	},
	timeSeries: {
		options: timeSeries,
		selectedSeries: 7,
		setSeries: () => null,
		getSeriesInfo: () => { }
	},
	byForm: {
		forms: {},
		hasForms: false,
		selectedForm: undefined,
		setSelectedForm: (_: string) => null
	},
	byAssignee: {
		assignees: {},
		hasAssignees: false,
		selectedAssignee: undefined,
		setSelectedAssignee: (_: string) => null
	},
	byDomain: {
		domains: {},
		hasDomains: false,
		selectedDomain: undefined,
		setSelectedDomain: (_: string) => null
	},
	sectionLoadStates: {
		stats: false,
		overallProgress: false,
		forms: false,
		questions: false,
		assignees: false,
		domains: false,
		setLoadStates: (_: any, __: any, ___: boolean) => null,
		resetLoadStates: () => null
	}
});

// Hook to use the context
export const useFormAnalyticsContext = () => useContext(FormAnalyticsContext);

// Provider Props
interface Props {
	children?: ReactNode | undefined;
}

// Provider component
export const FormAnalyticsProvider = ({ children }: Props) => {
	const [tab, setTab] = useState<string>('overall');
	const [series, setSeries] = useState<number>(7);
	const [selectedForm, setSelectedForm] = useState<string | undefined>();
	const [selectedAssignee, setSelectedAssignee] = useState<string | undefined>();
	const [selectedDomain, setSelectedDomain] = useState<string | undefined>();

	// Default waterfall load stats 
	const defaultLoadStates = {
		stats: {
			completedTrend: false,
			inProgressTrend: false,
			notStartedTrend: false
		},
		overallProgress: {
			docProgress: false
		},
		forms: {
			progressByForm: false,
			formTopPerforming: false,
			// formLeastPerforming: false
		},
		questions: {
			questions: false,
		},
		assignees: {
			progressByAssignee: false,
			// assigneeTopPerforming: false,
			// assigneeLeastPerforming: false
		},
		domains: {
			progressByDomain: false,
			domainTopPerforming: false,
			// domainLeastPerforming: false
		}
	}

	// Waterfall render
	const [loadStates, setLoadStates] = useState<any>(defaultLoadStates);

	// Set the load states
	const handleSetLoadStates = (group: any, key: any, value: boolean) => {
		if (loadStates[group][key] !== value) {
			setLoadStates({
				...loadStates,
				[group]: {
					...loadStates[group],
					[key]: value
				}
			});
		}
	};

	// Reset load states
	const resetLoadStates = () => setLoadStates(defaultLoadStates);

	// Handle switch the tabs 
	const handleSwitchTabs = (t: string) => {
		setTab(t);
		resetLoadStates();
	};

	// Handle switch the time series
	const handleSwitchSeries = (s: number) => {
		setSeries(s);
		resetLoadStates();
	}

	// Get date for queries from timeseries		
	const daysSinceDate = dayjs().subtract(series, 'day').format('YYYY-MM-DD');

	// Fetch max snapshot date
	const { data: snapshot, loading: snapshotLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': `select max(snapshot_date) from '{{ table }}'` } },
	});

	// Is the integration service available/online?
	const integrationServiceOffline = !snapshotLoading && snapshot?.formAnalytics?.errors !== null;

	// Define sql queries
	const snapshotDate = !integrationServiceOffline && snapshot?.formAnalytics?.table![0]?.row[0]?.value as string || undefined;
	const sql = sqlQueries(daysSinceDate, selectedForm, selectedAssignee, selectedDomain, snapshotDate, tab, series);

	// Fetch all the forms available 
	const { data: formsWithAnalytics, loading: formsWithAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getFormsWithAnalytics } },
		skip: integrationServiceOffline || snapshotLoading || !snapshotDate
	});

	// Fetch all the assigness available
	const { data: assignessWithFormAnalytics, loading: assignessWithFormAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getAssignessWithFormAnalytics } },
		skip: integrationServiceOffline || snapshotLoading || !snapshotDate
	});

	// Fetch all the domains available
	const { data: domainsWithFormAnalytics, loading: domainsWithFormAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getDomainsWithFormAnalytics } },
		skip: integrationServiceOffline || snapshotLoading || !snapshotDate
	});

	// Data items
	const forms = formsWithAnalytics?.formAnalytics?.table;
	const assignees = assignessWithFormAnalytics?.formAnalytics?.table;
	const domains = domainsWithFormAnalytics?.formAnalytics?.table;

	// Set the first form as selected if none is selected
	useEffect(() => {
		if (forms && forms.length > 0 && !selectedForm) {
			const row = forms[0].row as any;
			if (row) setSelectedForm(row[0].value as string); // form_id
		}
	}, [forms, selectedForm]);

	// Set the first assignee as selected if none is selected
	useEffect(() => {
		if (assignees && assignees.length > 0 && !selectedAssignee) {
			const row = assignees[0].row as any;
			if (row) setSelectedAssignee(row[0].value as string); // assignee_urn
		}
	}, [assignees, selectedAssignee]);

	// Set the first domain as selected if none is selected
	useEffect(() => {
		if (domains && domains.length > 0 && !selectedDomain) {
			const row = domains[0].row as any;
			if (row) setSelectedDomain(row[0].value as string); // domain
		}
	}, [domains, selectedDomain]);

	// Section load states (waterfall render)
	const sectionLoadStates = {
		stats:
			loadStates.stats.completedTrend
			&& loadStates.stats.inProgressTrend
			&& loadStates.stats.notStartedTrend,
		overallProgress: loadStates.overallProgress.docProgress,
		forms:
			loadStates.forms.progressByForm
			&& loadStates.forms.formTopPerforming,
		// && loadStates.forms.formLeastPerforming,
		questions: loadStates.questions.questions,
		assignees:
			loadStates.assignees.progressByAssignee,
		// && loadStates.assignees.assigneeTopPerforming,
		// && loadStates.assignees.assigneeLeastPerforming,
		domains:
			loadStates.domains.progressByDomain
			&& loadStates.domains.domainTopPerforming,
		// && loadStates.domains.domainLeastPerforming,
		setLoadStates: handleSetLoadStates,
		resetLoadStates,
	};

	// Return the context provider
	return (
		<FormAnalyticsContext.Provider value={{
			sql,
			integrationServiceOffline,
			contextLoading:
				snapshotLoading
				&& loadStates
				&& formsWithAnalyticsLoading
				&& assignessWithFormAnalyticsLoading
				&& domainsWithFormAnalyticsLoading,
			snapshot: snapshotDate,
			tabs: {
				selectedTab: tab,
				setSelectedTab: handleSwitchTabs
			},
			timeSeries: {
				options: timeSeries,
				selectedSeries: series,
				setSeries: handleSwitchSeries,
				getSeriesInfo: () => timeSeries.find((s) => s.key === series) || {},
			},
			byForm: {
				forms: formsWithAnalytics?.formAnalytics,
				hasForms: !formsWithAnalyticsLoading && forms ? forms.length > 0 : false,
				selectedForm,
				setSelectedForm
			},
			byAssignee: {
				assignees: assignessWithFormAnalytics?.formAnalytics,
				hasAssignees: !assignessWithFormAnalyticsLoading && assignees ? assignees.length > 0 : false,
				selectedAssignee,
				setSelectedAssignee
			},
			byDomain: {
				domains: domainsWithFormAnalytics?.formAnalytics,
				hasDomains: !domainsWithFormAnalyticsLoading && domains ? domains.length > 0 : false,
				selectedDomain,
				setSelectedDomain
			},
			sectionLoadStates,
		}}>{children}</FormAnalyticsContext.Provider>
	);
};