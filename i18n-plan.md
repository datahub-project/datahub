# i18n Extraction Plan — Dataset Tabs (without Validations)

## src/app/entityV2/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers.tsx

- [x] 8:35 (extract) — "You already have access to this role" — tooltip message constant `ACCESS_GRANTED_TOOLTIP` — entity.profile.access → t('accessManagement.accessGrantedTooltip')
- [x] 56:53 (extract) — "Granted" — button text returned from `getAccessButtonText` when `hasAccess` is true — entity.profile.access → t('accessManagement.granted')
- [x] 56:67 (extract) — "Request" — button text returned from `getAccessButtonText` when `hasAccess` is false — entity.profile.access → t('accessManagement.request')
- [x] 90:34 (extract) — "Access already granted" — aria-label on button when `hasAccess` is true — entity.profile.access → t('accessManagement.accessAlreadyGranted')
- [x] 90:62 (extract) — "Request access" — aria-label on button when `hasAccess` is false — entity.profile.access → t('accessManagement.requestAccess')
- [x] 92:13 (refactor) — "Granted" / "Request" — JSX conditional `{hasAccess ? 'Granted' : 'Request'}` duplicates the strings from `getAccessButtonText`; align with the extracted keys from line 56 so the same `t()` calls are reused here — entity.profile.access → t('accessManagement.granted') / t('accessManagement.request')

## src/app/entityV2/shared/tabs/Dataset/AccessManagement/AccessManagement.tsx

- [x] 76:17 (extract) — "Role Name" — table column title — entity.profile.access → t('accessManagement.columnRoleName')
- [x] 81:17 (extract) — "Description" — table column title — entity.profile.access → t('accessManagement.columnDescription')
- [x] 89:17 (extract) — "Access Type" — table column title — entity.profile.access → t('accessManagement.columnAccessType')
- [x] 94:17 (extract) — "Access" — table column title — entity.profile.access → t('accessManagement.columnAccess')
- [x] 109:21 (extract) — "Access management roles table" — `aria-label` on `StyledTable` — entity.profile.access → t('accessManagement.tableAriaLabel')

## src/app/entityV2/shared/tabs/Dataset/AccessManagement/AccessManagerDescription.tsx

- [x] 34:46 (extract) — " Read Less" — link text (note leading space; trim in extraction or preserve via key) — common.actions → tc('readLess')
- [x] 34:59 (extract) — "...Read More" — link text (ellipsis is part of the visible string) — common.actions → tc('readMore')

## src/app/entityV2/shared/tabs/Dataset/AccessManagement/utils.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/AddButton.tsx

- [x] 16:62 (extract) — "Add a highlighted query" — tooltip title shown when button is enabled (fallback branch of `(isButtonDisabled && ADD_UNAUTHORIZED_MESSAGE) || 'Add a highlighted query'`) — entity.profile.queries → t('queriesTab.addHighlightedQueryTooltip')

## src/app/entityV2/shared/tabs/Dataset/Queries/cacheUtils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/CopyQuery.tsx

- [x] 21:22 (extract) — "Copy the query" — tooltip title — entity.profile.queries → t('copyQuery.tooltip')
- [x] 23:39 (extract) — "Copied" — button text after successful copy — common.feedback → tc('copied')
- [x] 23:51 (extract) — "Copy" — button text before copy — common.actions → tc('copy')

## src/app/entityV2/shared/tabs/Dataset/Queries/EmptyQueriesSection.tsx

- [x] 96:37 (extract) — "No highlighted queries yet" — empty state description text inside `<Description>` — entity.profile.queries → t('emptyQueries.noHighlightedQueries')

## src/app/entityV2/shared/tabs/Dataset/Queries/Query.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderForm.tsx

- [x] 59:30 (extract) — "Query" — `Form.Item` label text inside `<Typography.Text strong>` — entity.profile.queries → t('queryBuilderModal.formLabelQuery')
- [x] 74:30 (extract) — "Title" — `Form.Item` label text inside `<Typography.Text strong>` — entity.profile.queries → t('queryBuilderModal.formLabelTitle')
- [x] 81:26 (extract) — "Join Transactions and Users Tables" — `placeholder` on title `<Input>` — entity.profile.queries → t('queryBuilderModal.titlePlaceholder')
- [x] 84:30 (extract) — "Description" — `Form.Item` label text inside `<Typography.Text strong>` — entity.profile.queries → t('queryBuilderModal.formLabelDescription')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderModal.tsx

- [x] 73:30 (extract) — "Created Query!" — `message.success` content — entity.profile.queries → t('queryBuilderModal.createSuccess')
- [x] 83:31 (extract) — "Failed to create Query! An unexpected error occurred" — `message.error` content — entity.profile.queries → t('queryBuilderModal.createError')
- [x] 111:30 (extract) — "Edited Query!" — `message.success` content — entity.profile.queries → t('queryBuilderModal.editSuccess')
- [x] 121:31 (extract) — "Failed to edit Query! An unexpected error occurred" — `message.error` content — entity.profile.queries → t('queryBuilderModal.editError')
- [x] 138:24 (extract) — "Edit Query" — modal title when `isUpdating` is true — entity.profile.queries → t('queryBuilderModal.editTitle')
- [x] 138:40 (extract) — "New Query" — modal title when `isUpdating` is false — entity.profile.queries → t('queryBuilderModal.newTitle')
- [x] 144:22 (extract) — "Cancel" — button text — common.actions → tc('cancel')
- [x] 151:22 (extract) — "Save" — button text — common.actions → tc('save')
- [x] 168:22 (extract) — "Exit Query Editor" — confirmation modal title (`modalTitle` prop) — entity.profile.queries → t('queryBuilderModal.exitConfirmTitle')
- [x] 169:21 (extract) — "Are you sure you want to exit the editor? Any unsaved changes will be lost." — confirmation modal body (`modalText` prop) — entity.profile.queries → t('queryBuilderModal.exitConfirmBody')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCard.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCardDetails.tsx

- [x] 99:13 (extract) — "No title" — fallback text when `title` is undefined: `{title || 'No title'}` — entity.profile.queries → t('queryCard.noTitle')
- [x] 119:77 (extract) — "more" — "read more" link text inside `NoMarkdownViewer` — common.actions → tc('more')
- [x] 123:27 (extract) — "No description" — fallback text in `<EmptyText>` — entity.profile.queries → t('queryCard.noDescription')
- [x] 127:44 (refactor) — "Created on {{date}}" — template literal `Created on ${toLocalDateString(createdAtMs)}`; refactor to use `t('key', { date: toLocalDateString(createdAtMs) })` with `{{date}}` placeholder — entity.profile.queries → t('queryCard.createdOn', { date: toLocalDateString(createdAtMs) })

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCardDetailsMenu.tsx

- [x] 29:30 (extract) — "Deleted Query!" — `message.success` content — entity.profile.queries → t('queryCard.deleteSuccess')
- [x] 37:31 (extract) — "Failed to delete Query! An unexpected error occurred" — `message.error` content — entity.profile.queries → t('queryCard.deleteError')
- [x] 51:40 (extract) — "Delete" — menu item label (note: preceded by icon + `&nbsp;`) — common.actions → tc('delete')
- [x] 63:22 (extract) — "Delete Query" — confirmation modal title (`modalTitle` prop) — entity.profile.queries → t('queryCard.deleteConfirmTitle')
- [x] 64:21 (extract) — "Are you sure you want to delete this query?" — confirmation modal body (`modalText` prop) — entity.profile.queries → t('queryCard.deleteConfirmBody')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCardEditButton.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCardHeader.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryCardQuery.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/queryColumns.tsx

- [x] 51:41 (extract) — "Read more" — link label for expanding truncated description — entity.profile.queries → t('queryCard.readMoreExpand')
- [x] 58:41 (extract) — "Read less" — link label for collapsing expanded description — entity.profile.queries → t('queryCard.readLessCollapse')
- [x] 120:30 (extract) — "Deleted Query!" — `message.success` content (same key as QueryCardDetailsMenu line 29) — entity.profile.queries → t('queryCard.deleteSuccess')
- [x] 128:31 (extract) — "Failed to delete Query! An unexpected error occurred" — `message.error` content (same key as QueryCardDetailsMenu line 37) — entity.profile.queries → t('queryCard.deleteError')
- [x] 164:22 (extract) — "Delete Query" — confirmation modal title (same key as QueryCardDetailsMenu line 63) — entity.profile.queries → t('queryCard.deleteConfirmTitle')
- [x] 165:21 (extract) — "Are you sure you want to delete this query?" — confirmation modal body (same key as QueryCardDetailsMenu line 64) — entity.profile.queries → t('queryCard.deleteConfirmBody')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryFilters/QueryFilters.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryFilters/useColumnsFilter.ts

- [x] 82:59 (extract) — "Columns" — `displayName` field of `columnsFilter` object passed to search filter component — entity.profile.queries → t('queryFilters.columnsDisplayName')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueryModal.tsx

- [x] 78:14 (extract) — "Query" — modal `title` prop — entity.profile.queries → t('queryBuilderModal.formLabelQuery')
- [x] 85:22 (extract) — "Close" — button text — common.actions → tc('close')
- [x] 103:20 (extract) — "No title" — fallback title: `{title || 'No title'}` (same key as QueryCardDetails line 99) — entity.profile.queries → t('queryCard.noTitle')
- [x] 105:52 (extract) — "No description" — fallback content: `description || 'No description'` (same key as QueryCardDetails line 123) — entity.profile.queries → t('queryCard.noDescription')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueriesListSection.tsx

- [x] 236:21 (extract) — "Add Highlighted Query" — `buttonLabel` prop passed to `AddButton` — entity.profile.queries → t('queriesTab.addHighlightedQueryButton')

## src/app/entityV2/shared/tabs/Dataset/Queries/QueriesTab.tsx

- [x] 125:28 (extract) — "Highlighted Queries" — `title` prop for `QueriesListSection` — entity.profile.queries → t('queriesTab.highlightedQueriesTitle')
- [x] 127:20 (extract) — "Curated queries relevant to this dataset" — `tooltip` prop for the Highlighted Queries section — entity.profile.queries → t('queriesTab.highlightedQueriesTooltip')
- [x] 142:28 (extract) — "Highlighted Queries" — `sectionName` prop for `EmptyQueriesSection` (same key as line 125) — entity.profile.queries → t('queriesTab.highlightedQueriesTitle')
- [x] 143:20 (extract) — "Curated queries relevant to this dataset" — `tooltip` prop for `EmptyQueriesSection` (same key as line 127) — entity.profile.queries → t('queriesTab.highlightedQueriesTooltip')
- [x] 146:21 (extract) — "Add Highlighted Query" — `buttonLabel` prop for `EmptyQueriesSection` (same key as QueriesListSection line 236) — entity.profile.queries → t('queriesTab.addHighlightedQueryButton')
- [x] 153:28 (extract) — "Downstream Queries" — `title` prop for Downstream `QueriesListSection` — entity.profile.queries → t('queriesTab.downstreamQueriesTitle')
- [x] 155:20 (extract) — "Queries that power downstream assets" — `tooltip` prop for Downstream section — entity.profile.queries → t('queriesTab.downstreamQueriesTooltip')
- [x] 163:28 (extract) — "Recent Queries" — `title` prop for Recent `QueriesListSection` — entity.profile.queries → t('queriesTab.recentQueriesTitle')
- [x] 165:20 (extract) — "Recently executed queries against this dataset" — `tooltip` prop for Recent section — entity.profile.queries → t('queriesTab.recentQueriesTooltip')

## src/app/entityV2/shared/tabs/Dataset/Queries/types.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/useDownstreamQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/useHighlightedQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/usePopularQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/useQueryTableColumns.tsx

- [x] 55:16 (extract) — "Title" — table column header (`title` field in `titleColumn`) — common.labels → tc('title')
- [x] 66:16 (extract) — "Description" — table column header (`title` field in `descriptionColumn`) — entity.profile.queries → t('queryCard.columnDescription')
- [x] 73:16 (extract) — "Query Text" — table column header (`title` field in `queryTextColumn`) — entity.profile.queries → t('queryCard.columnQueryText')
- [x] 101:16 (extract) — "Created By" — table column header (`title` field in `createdByColumn`) — entity.profile.queries → t('queryCard.columnCreatedBy')
- [x] 118:16 (extract) — "Date Created" — table column header (`title` field in `createdDateColumn`) — entity.profile.queries → t('queryCard.columnDateCreated')
- [x] 129:16 (extract) — "Powers" — table column header (`title` field in `powersColumn`) — entity.profile.queries → t('queryCard.columnPowers')
- [x] 149:16 (extract) — "Top Users" — table column header (`title` field in `topUsersColumn`) — entity.profile.queries → t('queryCard.columnTopUsers')
- [x] 171:16 (extract) — "Columns" — table column header (`title` field in `columnsColumn`) — common.labels → tc('columns')

## src/app/entityV2/shared/tabs/Dataset/Queries/useRecentQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/utils/constants.ts

- [x] 16:36 (extract) — "You are not authorized to add Queries to this entity." — exported constant `ADD_UNAUTHORIZED_MESSAGE` used as tooltip text in `AddButton.tsx` — entity.profile.queries → t('queriesTab.addUnauthorizedMessage')

## src/app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/utils/getTopNQueries.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Queries/utils/mapQuery.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/CompactSchemaTable.tsx

- [x] 126:15 (extract) — "Name" — column header title — common.labels → tc('name')
- [x] 144:15 (extract) — "Description" — column header title — common.labels → tc('description')
- [x] 172:15 (extract) — "Usage" — column header title — common.labels → tc('usage')
- [x] 220:20 (refactor) — `View {rows.length - numberOfRowsToShow} More` — JSX interpolation with dynamic count; refactor to `t('schema.viewMore', { count: rows.length - numberOfRowsToShow })` which renders as e.g. "View 3 More" — entity.profile.schema → t('compactSchemaTable.viewMore', { count: rows.length - numberOfRowsToShow })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/ConstraintLabels.tsx

- [x] 30:32 (extract) — "Primary Key" — pill label — entity.profile.schema → t('constraintLabels.primaryKey')
- [x] 34:32 (extract) — "Foreign Key" — pill label — entity.profile.schema → t('constraintLabels.foreignKey')
- [x] 38:32 (extract) — "Partition Key" — pill label — entity.profile.schema → t('constraintLabels.partitionKey')
- [x] 42:32 (extract) — "Nullable" — pill label — entity.profile.schema → t('constraintLabels.nullable')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/ExpandIcon.tsx

- [x] 126:28 (refactor) — `` `${record.depth + 1} level${record.depth === 0 ? '' : 's'} nested` `` — tooltip title with pluralization; refactor to use `t('schema.expandIcon.levelsNested', { count: record.depth + 1 })` with plural forms — entity.profile.schema → t('expandIcon.levelsNested', { count: record.depth + 1 })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/MenuColumn.tsx

- [x] 48:28 (extract) — "See Column Lineage" — menu item label — entity.profile.schema → t('menuColumn.seeColumnLineage')
- [x] 57:28 (extract) — "Copy Column Field Path" — menu item label — entity.profile.schema → t('menuColumn.copyColumnFieldPath')
- [x] 68:28 (extract) — "Copy Column Urn" — menu item label — entity.profile.schema → t('menuColumn.copyColumnUrn')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/AboutFieldTab.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/DrawerFooter.tsx

- [x] 82:20 (refactor) — `{expandedFieldIndex + 1} of {displayedRows.length} {pluralize(displayedRows.length, 'field')}` — navigation counter label; uses `pluralize()` helper inline. Refactor: replace with `t('fieldNavigation', '{{current}} of {{total}} {{fieldLabel}}', { current: expandedFieldIndex + 1, total: displayedRows.length, fieldLabel: pluralize(displayedRows.length, 'field') })` or use a dedicated translation key that handles pluralization via i18next plural forms. — entity.profile.schema → t('fieldDrawer.fieldNavigation', { current: expandedFieldIndex + 1, total: displayedRows.length, fieldLabel: pluralize(displayedRows.length, 'field') })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldBusinessAttribute.tsx

- [x] 38:36 (extract) — `"Business Attribute"` — sidebar section title prop — entity.profile.schema → t('fieldBusinessAttribute.sectionTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDescription.tsx

- [x] 80:22 (extract) — `'Updated!'` — toast success message — entity.profile.schema → t('fieldDescription.updated')
- [x] 85:47 (refactor) — `` `Proposal Failed! \n ${e.message || ''}` `` — toast error message with interpolation; refactor to `t('proposalFailed', 'Proposal Failed!\n{{message}}', { message: e.message || '' })` — entity.profile.schema → t('fieldDescription.proposalFailed', { message: e.message || '' })
- [x] 115:24 (extract) — `"Description"` — sidebar section title prop — entity.profile.schema → t('fieldDescription.sectionTitle')
- [x] 139:28 (extract) — `"Add Description"` — add-description button text — entity.profile.schema → t('fieldDescription.addDescription')
- [x] 157:24 (extract) — `'Update description'` — modal title (conditional true branch) — entity.profile.schema → t('fieldDescription.updateDescriptionTitle')
- [x] 157:46 (extract) — `'Add description'` — modal title (conditional false branch) — entity.profile.schema → t('fieldDescription.addDescriptionTitle')
- [x] 163:30 (extract) — `'Updating...'` — toast loading message — entity.profile.schema → t('fieldDescription.updating')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDetails.tsx

- [x] 122:21 (extract) — `"Popularity"` — detail label text — entity.profile.schema → t('fieldDetails.popularity')
- [x] 133:21 (extract) — `"Notes"` — detail label text — entity.profile.schema → t('fieldDetails.notes')
- [x] 143:28 (extract) — `"+ Add Note"` — button label — entity.profile.schema → t('fieldDetails.addNote')
- [x] 148:21 (extract) — `"Deprecation"` — detail label text — entity.profile.schema → t('fieldDetails.deprecation')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldHeader.tsx

- [x] 131:21 (extract) — `"Column"` — field type label displayed below field name — entity.profile.schema → t('fieldHeader.column')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldPath.tsx

- [x] 78:21 (refactor) — `{displayNameTokens.length} levels nested` — nesting depth label with dynamic count; refactor to `t('levelsNested', '{{count}} levels nested', { count: displayNameTokens.length })` — entity.profile.schema → t('fieldPath.levelsNested', { count: displayNameTokens.length })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldPopularity.tsx

- [x] 36:18 (refactor) — `` `${formatNumberWithoutAbbreviation(relevantUsageStats.count || 0)} queries / month` `` — tooltip text with interpolated count; refactor to `t('queriesPerMonth', '{{count}} queries / month', { count: formatNumberWithoutAbbreviation(relevantUsageStats.count || 0) })` — entity.profile.schema → t('fieldPopularity.queriesPerMonth', { count: formatNumberWithoutAbbreviation(relevantUsageStats.count || 0) })
- [x] 37:18 (extract) — `'No column usage data'` — tooltip text when no usage data — entity.profile.schema → t('fieldPopularity.noUsageData')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTags.tsx

- [x] 29:36 (extract) — `"Tags"` — sidebar section title prop — entity.profile.schema → t('fieldTags.sectionTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTerms.tsx

- [x] 30:24 (extract) — `"Glossary Terms"` — sidebar section title prop — entity.profile.schema → t('fieldTerms.sectionTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SampleValuesSection.tsx

- [x] 21:24 (extract) — `"Sample Values"` — sidebar section title prop — entity.profile.schema → t('fieldSamples.sectionTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer.tsx

- [x] 107:36 (skip) — `'About'` — default tab name / tab label in tabs array — entity.profile.schema → t('fieldDrawerTabs.about') ⚠ not found
- [x] 185:20 (skip) — `'About'` — tab `name` field in tabs array definition — entity.profile.schema → t('fieldDrawerTabs.about') ⚠ not found
- [x] 206:20 (skip) — `'Statistics'` — tab `name` field in tabs array definition — entity.profile.schema → t('fieldDrawerTabs.statistics') ⚠ not found
- [x] 218:20 (skip) — `'Queries'` — tab `name` field in tabs array definition — entity.profile.schema → t('fieldDrawerTabs.queries') ⚠ not found
- [x] 219:26 (extract) — `'View queries about this field'` — tab `description` field — entity.profile.schema → t('fieldDrawerTabs.queriesDescription')
- [x] 225:20 (skip) — `'Properties'` — tab `name` field in tabs array definition — entity.profile.schema → t('fieldDrawerTabs.properties') ⚠ not found
- [x] 226:26 (extract) — `'View additional properties about this field'` — tab `description` field — entity.profile.schema → t('fieldDrawerTabs.propertiesDescription')
- [x] 306:33 (extract) — `"Timeline for table"` — drawer header text — entity.profile.schema → t('fieldDrawer.timelineForTable')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawerTabs.tsx

✓ no strings (tab names are passed in dynamically via `tab.name` from the parent; they are extracted in `SchemaFieldDrawer.tsx`)

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldQueriesSidebarTab.tsx

- [x] 148:26 (extract) — `"No queries for this column found"` — empty-state section label — entity.profile.schema → t('queriesSidebarTab.noQueriesFound')
- [x] 152:53 (extract) — `"Queries"` — section title text — entity.profile.schema → t('queriesSidebarTab.sectionTitle')
- [x] 161:25 (refactor) — `Last run by` / `on {dayjs(query.lastRun).format('MM/DD/YYYY')}` — JSX text fragments split around `<StyledCreatedBy>` component; refactor using `<Trans>`: `<Trans i18nKey="lastRunBy">Last run by <StyledCreatedBy>...</StyledCreatedBy> on {{date}}</Trans>` with `date={dayjs(query.lastRun).format('MM/DD/YYYY')}` — entity.profile.schema → t('queriesSidebarTab.lastRunBy', { date: dayjs(query.lastRun).format('MM/DD/YYYY') }) via Trans
- [x] 174:26 (extract) — `"See All Queries"` — button label — entity.profile.schema → t('queriesSidebarTab.seeAllQueries')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarColumnTab.tsx

- [x] 113:26 (extract) — `"No column statistics found"` — empty-state section label — entity.profile.schema → t('statsSidebar.noColumnStatsFound')
- [x] 126:24 (extract) — `"Null Count Over Time"` — chart title prop — entity.profile.schema → t('statsSidebar.nullCountOverTime')
- [x] 133:24 (extract) — `"Null Percentage Over Time"` — chart title prop — entity.profile.schema → t('statsSidebar.nullPercentageOverTime')
- [x] 140:24 (extract) — `"Distinct Count Over Time"` — chart title prop — entity.profile.schema → t('statsSidebar.distinctCountOverTime')
- [x] 147:24 (extract) — `"Distinct Percentage Over Time"` — chart title prop — entity.profile.schema → t('statsSidebar.distinctPercentageOverTime')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarContent.tsx

- [x] 120:26 (extract) — `"No column statistics found"` — empty-state section label (duplicate of StatsSidebarColumnTab; share key) — entity.profile.schema → t('statsSidebar.noColumnStatsFound')
- [x] 128:22 (extract) — `'Null Count'` — stat name in statsData array — entity.profile.schema → t('statsSummary.nullCount')
- [x] 132:22 (extract) — `'Null %'` — stat name in statsData array — entity.profile.schema → t('statsSummary.nullPercent')
- [x] 136:22 (extract) — `'Distinct Count'` — stat name in statsData array — entity.profile.schema → t('statsSummary.distinctCount')
- [x] 140:22 (extract) — `'Distinct %'` — stat name in statsData array — entity.profile.schema → t('statsSummary.distinctPercent')
- [x] 144:22 (extract) — `'Min'` — stat name in statsData array — entity.profile.schema → t('statsSummary.min')
- [x] 148:22 (extract) — `'Max'` — stat name in statsData array — entity.profile.schema → t('statsSummary.max')
- [x] 152:22 (extract) — `'Median'` — stat name in statsData array — entity.profile.schema → t('statsSummary.median')
- [x] 156:22 (extract) — `'Std Dev'` — stat name in statsData array — entity.profile.schema → t('statsSummary.stdDev')
- [x] 159:22 (extract) — `'Sample Values'` — stat name in statsData array — entity.profile.schema → t('statsSummary.sampleValues')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarHeader.tsx

- [x] 99:21 (extract) — `"Stats & Insights"` — tab switch button label — entity.profile.schema → t('statsSidebar.statsAndInsights')
- [x] 105:21 (extract) — `"Historical Stats"` — tab switch button label — entity.profile.schema → t('statsSidebar.historicalStats')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView.tsx

- [x] 68:9 (refactor) — `` `Reported on ${toLocalDateString(...)} at ${toLocalTimeString(...)}` `` — reported-at timestamp label; refactor to `t('reportedAt', 'Reported on {{date}} at {{time}}', { date: toLocalDateString(latestProfile?.timestampMillis), time: toLocalTimeString(latestProfile?.timestampMillis) })` — entity.profile.schema → t('statsSidebar.reportedAt', { date: toLocalDateString(latestProfile?.timestampMillis), time: toLocalTimeString(latestProfile?.timestampMillis) })
- [x] 95:22 (extract) — `"Loading..."` — loading indicator text — common.feedback → tc('loading')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSummaryRow.tsx

- [x] 35:25 (refactor) — `` `${nullCount} null ${pluralize(nullCount || 0, 'value')} found for this column across rows` `` — tooltip title with interpolation and pluralization; refactor to `t('nullValuesTooltip', '{{count}} null {{valueLabel}} found for this column across rows', { count: nullCount, valueLabel: pluralize(nullCount || 0, 'value') })` — entity.profile.schema → t('statsSummary.nullValuesTooltip', { count: nullCount, valueLabel: pluralize(nullCount || 0, 'value') })
- [x] 39:22 (extract) — `"Null Values"` — headline label prop — entity.profile.schema → t('statsSummary.nullValues')
- [x] 44:25 (refactor) — `` `${uniqueCount} unique ${pluralize(uniqueCount || 0, 'value')} found for this column across rows` `` — tooltip title with interpolation and pluralization; refactor to `t('uniqueValuesTooltip', '{{count}} unique {{valueLabel}} found for this column across rows', { count: uniqueCount, valueLabel: pluralize(uniqueCount || 0, 'value') })` — entity.profile.schema → t('statsSummary.uniqueValuesTooltip', { count: uniqueCount, valueLabel: pluralize(uniqueCount || 0, 'value') })
- [x] 48:22 (extract) — `"Distinct Values"` — headline label prop — entity.profile.schema → t('statsSummary.distinctValues')
- [x] 54:22 (extract) — `"Numerical stats"` — headline label prop — entity.profile.schema → t('statsSummary.numericalStats')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsTabWrapper.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/Loading.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/NoStats.tsx

- [x] 27:18 (extract) — `"No column statistics found"` — empty-state message shown when no stats are available — entity.profile.schema → t('statsV2.noColumnStatsFound')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricChartPopover.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart.tsx

- [x] 61:17 (refactor) — `` `${capitalizeFirstLetterOnly(metric)} Value` `` — fallback label in popover rendered as JSX text; currently a template literal producing e.g. "UniqueCount Value". Extract by replacing with `t('statsV2.metricValueLabel', { metric: capitalizeFirstLetterOnly(metric) })` with EN value `"{{metric}} Value"`. — entity.profile.schema → t('statsV2Charts.metricValueLabel', { metric: capitalizeFirstLetterOnly(metric) })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart.tsx

- [x] 51:17 (refactor) — `{pluralize(datum.y, 'Row')}` — the string `'Row'` is the singular word passed to a pluralize utility that produces "Row" / "Rows". It is user-visible text inside a JSX expression. Extract the base word: `t('statsV2.rowLabel')` → `"Row"` (the pluralize helper appends the "s"). Note: confirm that `pluralize` accepts a pre-translated string; if it always appends "s" this is straightforward. — entity.profile.schema → t('statsV2Charts.rowLabel') (pluralize helper receives the translated string)

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/RowCountByValuePopover.tsx

- [x] 56:57 (refactor) — `{pluralize(datum.x, 'Row')}` — same pattern as above; user-visible singular word passed to pluralize. Extract `'Row'` → `t('statsV2.rowLabel')`. — entity.profile.schema → t('statsV2Charts.rowLabel')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/DistinctValuesChart.tsx

- [x] 10:16 (extract) — `"Distinct Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.distinctValuesTitle')
- [x] 11:20 (extract) — `"Number of rows with distinct values for this column"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.distinctValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useDefaultLookbackWindowType.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useExtractMetricStats.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useGetBottomAxisPropsByLookbackWindowType.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/usePrepareMetricStats.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MaxValuesChart.tsx

- [x] 14:36 (extract) — `"Max Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.maxValuesTitle')
- [x] 14:55 (extract) — `"Max values for this column over time"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.maxValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MeanValuesChart.tsx

- [x] 14:36 (extract) — `"Mean Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.meanValuesTitle')
- [x] 14:56 (extract) — `"Mean values for this column over time"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.meanValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MedianValuesChart.tsx

- [x] 14:36 (extract) — `"Median Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.medianValuesTitle')
- [x] 14:58 (extract) — `"Median values for this column over time"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.medianValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MinValuesChart.tsx

- [x] 17:20 (extract) — `"Min Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.minValuesTitle')
- [x] 18:24 (extract) — `"Min values for this column over time"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.minValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/NullValuesChart.tsx

- [x] 14:16 (extract) — `"Null Values"` — chart title prop — entity.profile.schema → t('statsV2Charts.nullValuesTitle')
- [x] 15:20 (extract) — `"Number of rows with a null value for this column"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.nullValuesSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/NumericDistributionChart.tsx

- [x] 35:16 (extract) — `"Numeric Column Distribution"` — chart title prop — entity.profile.schema → t('statsV2Charts.numericDistributionTitle')
- [x] 36:20 (extract) — `"Numeric distribution for this column"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.numericDistributionSubtitle')
- [x] 38:67 (extract) — `"Column Value"` — axis label prop passed to `WhiskerChart` — entity.profile.schema → t('statsV2Charts.columnValueAxisLabel')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/RowCountByValueChart.tsx

- [x] 55:16 (extract) — `"Row Count by Value"` — chart title prop — entity.profile.schema → t('statsV2Charts.rowCountByValueTitle')
- [x] 56:20 (extract) — `"Number of rows with each distinct value"` — chart subtitle prop — entity.profile.schema → t('statsV2Charts.rowCountByValueSubtitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/ChartsSection.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/AllSamplesDrawer.tsx

- [x] 17:24 (extract) — `"Sample Values"` — drawer title prop — entity.profile.schema → t('statsV2Samples.allSamplesDrawerTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleDrawer.tsx

- [x] 14:24 (extract) — `"Sample Value"` — drawer title prop — entity.profile.schema → t('statsV2Samples.sampleDrawerTitle')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SamplesTable.tsx

- [x] 21:20 (extract) — `"Sample Values"` — table column header (`title` prop of `Column`) — entity.profile.schema → t('statsV2Samples.columnHeader')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleValueCell.tsx

- [x] 63:25 (extract) — `"View"` — button label for expanding a truncated sample value — entity.profile.schema → t('statsV2Samples.viewButton')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleValueDetailed.tsx

- [x] 31:9 (extract) — `'Copied!'` — `message.success` toast shown after copying sample to clipboard — common.feedback → tc('copiedSuccess')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/SamplesSection.tsx

- [x] 49:25 (refactor) — `View {numberOfHiddenSampleValues} more` — button label with embedded dynamic count. Refactor to `t('statsV2.viewMoreSamples', { count: numberOfHiddenSampleValues })` → `"View {{count}} more"`. — entity.profile.schema → t('statsV2Samples.viewMore', { count: numberOfHiddenSampleValues })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/utils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Header.tsx

- [x] 25:13 (extract) — `"Stats & Insights"` — section heading — entity.profile.schema → t('statsV2Insights.sectionHeading')
- [x] 26:17 (refactor) — `Last Updated: {lastUpdatedAtString}` — label with dynamic timestamp value. Refactor to `t('statsV2.lastUpdated', { timestamp: lastUpdatedAtString })` → `"Last Updated: {{timestamp}}"`. — entity.profile.schema → t('statsV2Insights.lastUpdated', { timestamp: lastUpdatedAtString })

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Metric.tsx

✓ no strings (label and value are passed in as props; the caller is responsible for translating them)

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Metrics.tsx

- [x] 32:12 (extract) — `'Null Count'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.nullCount')
- [x] 38:12 (extract) — `'Null %'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.nullPercent')
- [x] 48:12 (extract) — `'Distinct Count'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.distinctCount')
- [x] 53:12 (extract) — `'Distinct %'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.distinctPercent')
- [x] 63:12 (extract) — `'Max'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.max')
- [x] 80:12 (extract) — `'Min'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.min')
- [x] 96:12 (extract) — `'Median'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.median')
- [x] 106:12 (extract) — `'Standard Deviation'` — metric label in `METRICS` array — entity.profile.schema → t('statsV2Insights.standardDeviation')

Note: These labels are defined in the `METRICS` constant array outside of the render function. They are used as `metric.label` at render time. The `label` field must be changed to hold the translated string (computed via `t()` inside the component or by converting `METRICS` to a function that receives `t`).

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/StatsAndInsightsSection.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContent.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContext.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/utils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/TrendDetail.tsx

- [x] 59:28 (refactor) — `` `${count} stats` `` — display text for numerical stats count; refactor to `t('statsCount', '{{count}} stats', { count })` — entity.profile.schema → t('statsSidebar.statsCount', { count })
- [x] 62:28 (refactor) — `` `${count} values` `` — display text for value count; refactor to `t('valuesCount', '{{count}} values', { count })` — entity.profile.schema → t('statsSidebar.valuesCount', { count })
- [x] 65:28 (extract) — `'None'` — display text when count < 1 — entity.profile.schema → t('statsSidebar.none')

## src/app/entityV2/shared/tabs/Dataset/Schema/components/TypeLabel.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString.ts

- [x] 25:1 (skip) — `'A new field'` — default field path constant used as a fragment inside larger sentences; word order differs by language ⚠ not found
- [x] 26:1 (skip) — `'Asset documentation is empty.'` — see line 26:1 extract below
- [x] 27:1 (refactor) — `` `Field documentation for ${downgradeV2FieldPath(fieldPath)} is empty.` `` — template literal full sentence; refactor constant to a `t()` call with `fieldPath` interpolation: `t('schema.history.fieldDocEmpty', { fieldPath: downgradeV2FieldPath(fieldPath) })` — entity.profile.schema → t('changeEventString.fieldDocEmpty', { fieldPath: downgradeV2FieldPath(fieldPath) })
- [x] 28:1 (refactor) — `` `Set asset documentation to ${description}` `` — template literal full sentence; refactor to `t('schema.history.setAssetDoc', { description })` — entity.profile.schema → t('changeEventString.setAssetDoc', { description })
- [x] 29:1 (refactor) — `` `Set field documentation for ${downgradeV2FieldPath(fieldPath)} to ${description}` `` — template literal full sentence; refactor to `t('schema.history.setFieldDoc', { fieldPath: downgradeV2FieldPath(fieldPath), description })` — entity.profile.schema → t('changeEventString.setFieldDoc', { fieldPath: downgradeV2FieldPath(fieldPath), description })
- [x] 26:1 (extract) — `'Asset documentation is empty.'` — complete standalone sentence constant; extract to `t('schema.history.assetDocEmpty')` — entity.profile.schema → t('changeEventString.assetDocEmpty')
- [x] 108:9 (refactor) — `` `Added column ${downgradeV2FieldPath(fieldPath || '')}.` `` — full sentence; refactor to `t('schema.history.addedColumn', { column: downgradeV2FieldPath(fieldPath || '') })` — entity.profile.schema → t('changeEventString.addedColumn', { column: downgradeV2FieldPath(fieldPath || '') })
- [x] 110:9 (refactor) — `` `Removed column ${downgradeV2FieldPath(fieldPath || '')}.` `` — full sentence; refactor to `t('schema.history.removedColumn', { column: downgradeV2FieldPath(fieldPath || '') })` — entity.profile.schema → t('changeEventString.removedColumn', { column: downgradeV2FieldPath(fieldPath || '') })
- [x] 112:9 (refactor) — `` `Modified column ${downgradeV2FieldPath(fieldPath || '')}.` `` — full sentence; refactor to `t('schema.history.modifiedColumn', { column: downgradeV2FieldPath(fieldPath || '') })` — entity.profile.schema → t('changeEventString.modifiedColumn', { column: downgradeV2FieldPath(fieldPath || '') })
- [x] 135:9 (skip) — `'Unknown'` — default fallback for missing URN name; used inside sentence fragments with dynamic surrounding text; skip individual constant; handle at sentence level ⚠ not found
- [x] 140:13 (refactor) — `` `Added tag "${tagName}" to field ${downgradeV2FieldPath(fieldPath)}.` `` — full sentence; refactor to `t('schema.history.addedTagToField', { tagName, field: downgradeV2FieldPath(fieldPath) })` — entity.profile.schema → t('changeEventString.addedTagToField', { tagName, field: downgradeV2FieldPath(fieldPath) })
- [x] 141:13 (refactor) — `` `Added tag "${tagName}".` `` — full sentence; refactor to `t('schema.history.addedTag', { tagName })` — entity.profile.schema → t('changeEventString.addedTag', { tagName })
- [x] 144:13 (refactor) — `` `Removed tag "${tagName}" from field ${downgradeV2FieldPath(fieldPath)}.` `` — full sentence; refactor to `t('schema.history.removedTagFromField', { tagName, field: downgradeV2FieldPath(fieldPath) })` — entity.profile.schema → t('changeEventString.removedTagFromField', { tagName, field: downgradeV2FieldPath(fieldPath) })
- [x] 145:13 (refactor) — `` `Removed tag "${tagName}".` `` — full sentence; refactor to `t('schema.history.removedTag', { tagName })` — entity.profile.schema → t('changeEventString.removedTag', { tagName })
- [x] 155:13 (refactor) — `` `Added ${label} term "${termName}".` `` — full sentence; refactor to `t('schema.history.addedLabelTerm', { label, termName })` — entity.profile.schema → t('changeEventString.addedLabelTerm', { label, termName })
- [x] 157:13 (refactor) — `` `Removed ${label} term "${termName}".` `` — full sentence; refactor to `t('schema.history.removedLabelTerm', { label, termName })` — entity.profile.schema → t('changeEventString.removedLabelTerm', { label, termName })
- [x] 163:17 (refactor) — `` `Added term "${termName}" to field ${downgradeV2FieldPath(fieldPath)}.` `` — full sentence; refactor to `t('schema.history.addedTermToField', { termName, field: downgradeV2FieldPath(fieldPath) })` — entity.profile.schema → t('changeEventString.addedTermToField', { termName, field: downgradeV2FieldPath(fieldPath) })
- [x] 164:17 (refactor) — `` `Added term "${termName}".` `` — full sentence; refactor to `t('schema.history.addedTerm', { termName })` — entity.profile.schema → t('changeEventString.addedTerm', { termName })
- [x] 167:17 (refactor) — `` `Removed term "${termName}" from field ${downgradeV2FieldPath(fieldPath)}.` `` — full sentence; refactor to `t('schema.history.removedTermFromField', { termName, field: downgradeV2FieldPath(fieldPath) })` — entity.profile.schema → t('changeEventString.removedTermFromField', { termName, field: downgradeV2FieldPath(fieldPath) })
- [x] 168:17 (refactor) — `` `Removed term "${termName}".` `` — full sentence; refactor to `t('schema.history.removedTerm', { termName })` — entity.profile.schema → t('changeEventString.removedTerm', { termName })
- [x] 184:13 (refactor) — `` `Added owner "${ownerName}"${ownerTypeSuffix}.` `` — full sentence; refactor to `t('schema.history.addedOwner', { ownerName, ownerTypeSuffix })` — entity.profile.schema → t('changeEventString.addedOwner', { ownerName, ownerTypeSuffix })
- [x] 186:13 (refactor) — `` `Removed owner "${ownerName}"${ownerTypeSuffix}.` `` — full sentence; refactor to `t('schema.history.removedOwner', { ownerName, ownerTypeSuffix })` — entity.profile.schema → t('changeEventString.removedOwner', { ownerName, ownerTypeSuffix })
- [x] 192:13 (refactor) — `` `Added to domain "${domainName}".` `` — full sentence; refactor to `t('schema.history.addedToDomain', { domainName })` — entity.profile.schema → t('changeEventString.addedToDomain', { domainName })
- [x] 194:13 (refactor) — `` `Removed from domain "${domainName}".` `` — full sentence; refactor to `t('schema.history.removedFromDomain', { domainName })` — entity.profile.schema → t('changeEventString.removedFromDomain', { domainName })
- [x] 204:13 (refactor) — `` `Set structured property "${propertyName}"${valuesSuffix}.` `` — full sentence; refactor to `t('schema.history.setStructuredProperty', { propertyName, valuesSuffix })` — entity.profile.schema → t('changeEventString.setStructuredProperty', { propertyName, valuesSuffix })
- [x] 206:13 (refactor) — `` `Removed structured property "${propertyName}".` `` — full sentence; refactor to `t('schema.history.removedStructuredProperty', { propertyName })` — entity.profile.schema → t('changeEventString.removedStructuredProperty', { propertyName })
- [x] 208:13 (refactor) — `` `Updated structured property "${propertyName}"${valuesSuffix}.` `` — full sentence; refactor to `t('schema.history.updatedStructuredProperty', { propertyName, valuesSuffix })` — entity.profile.schema → t('changeEventString.updatedStructuredProperty', { propertyName, valuesSuffix })
- [x] 215:13 (refactor) — `` `Added to application "${appName}".` `` — full sentence; refactor to `t('schema.history.addedToApplication', { appName })` — entity.profile.schema → t('changeEventString.addedToApplication', { appName })
- [x] 217:13 (refactor) — `` `Removed from application "${appName}".` `` — full sentence; refactor to `t('schema.history.removedFromApplication', { appName })` — entity.profile.schema → t('changeEventString.removedFromApplication', { appName })
- [x] 223:13 (refactor) — `` `Added asset "${assetName}".` `` — full sentence; refactor to `t('schema.history.addedAsset', { assetName })` — entity.profile.schema → t('changeEventString.addedAsset', { assetName })
- [x] 225:13 (refactor) — `` `Removed asset "${assetName}".` `` — full sentence; refactor to `t('schema.history.removedAsset', { assetName })` — entity.profile.schema → t('changeEventString.removedAsset', { assetName })

## src/app/entityV2/shared/tabs/Dataset/Schema/history/ChangeEvent.tsx

- [x] 68:24 (extract) — "Show less" — toggle link text when expanded — common.actions → tc('showLess')
- [x] 68:38 (extract) — "Show more" — toggle link text when collapsed — common.actions → tc('showMore')

## src/app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView.tsx

- [x] 129:52 (extract) — `by {actorName}` — actor attribution text; the word "by" needs translation. Refactor to `t('schema.history.byActor', { actorName })` using Trans or interpolation — entity.profile.schema → t('historyTransaction.byActor', { actorName })

## src/app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.tsx

- [x] 215:20 (extract) — "Change History" — drawer header title — entity.profile.schema → t('historySidebar.changeHistory')
- [x] 222:24 (extract) — "Search changes..." — search bar placeholder — entity.profile.schema → t('historySidebar.searchChangesPlaceholder')
- [x] 229:24 (extract) — "Filter" — select placeholder — entity.profile.schema → t('historySidebar.filterPlaceholder')
- [x] 229:52 (extract) — "Types" — select label — entity.profile.schema → t('historySidebar.typesLabel')
- [x] 253:28 (extract) — "Unable to load change history." — error state footer message — entity.profile.schema → t('historySidebar.unableToLoad')
- [x] 255:28 (extract) — "Showing the most recent changes. Older history may not be displayed." — truncation notice footer message — entity.profile.schema → t('historySidebar.truncationNotice')
- [x] 257:28 (extract) — "Complete change history" — footer message when full history is shown — entity.profile.schema → t('historySidebar.completeHistory')

## src/app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils.ts

- [x] 55:46 (extract) — `'Schema'` — category option label — entity.profile.schema → t('historySidebar.categorySchema')
- [x] 56:50 (extract) — `'Documentation'` — category option label — entity.profile.schema → t('historySidebar.categoryDocumentation')
- [x] 57:44 (extract) — `'Tags'` — category option label — entity.profile.schema → t('historySidebar.categoryTags')
- [x] 58:48 (extract) — `'Terms'` — category option label — entity.profile.schema → t('historySidebar.categoryTerms')
- [x] 59:50 (extract) — `'Owners'` — category option label — entity.profile.schema → t('historySidebar.categoryOwners')
- [x] 60:44 (extract) — `'Domains'` — category option label — entity.profile.schema → t('historySidebar.categoryDomains')
- [x] 61:51 (extract) — `'Properties'` — category option label — entity.profile.schema → t('historySidebar.categoryProperties')
- [x] 62:52 (extract) — `'Applications'` — category option label — entity.profile.schema → t('historySidebar.categoryApplications')
- [x] 63:50 (extract) — `'Assets'` — category option label — entity.profile.schema → t('historySidebar.categoryAssets')

## src/app/entityV2/shared/tabs/Dataset/Schema/history/historyUtils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/history/useResolveEntityNames.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/SchemaContext.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/SchemaTable.tsx

- [x] 237:15 (extract) — "Name" — column header title — common.labels → tc('name')
- [x] 252:15 (extract) — "Type" — column header title — entity.profile.schema → t('schemaTable.typeColumn')
- [x] 265:15 (extract) — "Description" — column header title — entity.profile.schema → t('schemaTable.descriptionColumn')
- [x] 279:15 (extract) — "Tags" — column header title — common.labels → tc('tags')
- [x] 292:15 (extract) — "Glossary Terms" — column header title — entity.profile.schema → t('schemaTable.glossaryTermsColumn')
- [x] 306:15 (extract) — "Business Attribute" — column header title — entity.profile.schema → t('schemaTable.businessAttributeColumn')
- [x] 331:15 (extract) — "Stats" — column header title — entity.profile.schema → t('schemaTable.statsColumn')

## src/app/entityV2/shared/tabs/Dataset/Schema/SchemaTab.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/useSchemaVersioning.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/getExpandedDrawerFieldPath.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/getSchemaFilterTypesFromUrl.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/queryStringUtils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/updateSchemaFilterQueryString.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useBusinessAttributeRenderer.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useDescriptionRenderer.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useGetStructuredPropColumns.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer.tsx

- [x] 59:25 (refactor) — `` `Some tags were sourced from ${platformName || 'an external platform'}. They will be resynced periodically during scheduled ingestion.` `` — tooltip title with dynamic platform name; refactor to `t('schema.tags.externalPlatformTooltip', { platform: platformName || t('schema.tags.externalPlatformFallback') })` — entity.profile.schema → t('tagTermRenderer.externalPlatformTooltip', { platform: platformName || t('tagTermRenderer.externalPlatformFallback') })
- [x] 63:28 (extract) — "Some tags are not editable." — disclaimer text in UI — entity.profile.schema → t('tagTermRenderer.tagsNotEditable')

## src/app/entityV2/shared/tabs/Dataset/Schema/utils/useUsageStatsRenderer.tsx

- [x] 53:58 (extract) — "No column statistics" — tooltip title when no field profile — entity.profile.schema → t('usageStatsRenderer.noColumnStats')
- [x] 53:80 (extract) — "Has column statistics" — tooltip title when field profile exists — entity.profile.schema → t('usageStatsRenderer.hasColumnStats')

## src/app/entityV2/shared/tabs/Dataset/Stats/constants.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/historical/charts/ProfilingRunsChart.tsx

- [x] 46:27 (extract) — `'unknown'` — fallback value shown in table cell when rowCount is absent; assigned to `rowCount` field passed directly to table data — entity.profile.stats → t('profilingRunsChart.unknown')
- [x] 47:30 (extract) — `'unknown'` — same pattern for columnCount — entity.profile.stats → t('profilingRunsChart.unknown') (same key)
- [x] 48:35 (extract) — `'unknown'` — same pattern for sizeInBytes — entity.profile.stats → t('profilingRunsChart.unknown') (same key)
- [x] 55:16 (extract) — `'Partition'` — table column title rendered when all profiles are partitioned — entity.profile.stats → t('profilingRunsChart.partitionColumn')
- [x] 55:44 (extract) — `'Date'` — table column title rendered when profiles are not partitioned — entity.profile.stats → t('profilingRunsChart.dateColumn')
- [x] 69:16 (extract) — `'Row Count'` — table column title — entity.profile.stats → t('profilingRunsChart.rowCountColumn')
- [x] 73:16 (extract) — `'Column Count'` — table column title — entity.profile.stats → t('profilingRunsChart.columnCountColumn')
- [x] 78:16 (extract) — `'Size'` — table column title — entity.profile.stats → t('profilingRunsChart.sizeColumn')
- [x] 88:13 (refactor) — `` `Showing profile from ${...} at ${...}` `` — `profileModalTitle` template literal. Contains interpolated date and time values. **Refactor:** use `t('datasetStats.profilingRuns.showingProfile', { date: ..., time: ... })` with `{{date}}` and `{{time}}` placeholders. — entity.profile.stats → t('profilingRunsChart.showingProfile', { date: ..., time: ... })
- [x] 98:28 (extract) — `'Profile'` — fallback modal title when `profileModalTitle` is falsy — entity.profile.stats → t('profilingRunsChart.profileFallbackTitle')

## src/app/entityV2/shared/tabs/Dataset/Stats/historical/charts/StatChart.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/historical/HistoricalStats.tsx

- [x] 99:28 (extract) — `'Stats for column '` — prefix text prop passed to `PrefixedSelect`; note trailing space is intentional for concatenation with the select value rendered inline. **Refactor note:** `PrefixedSelect` renders `prefixText` as a `<SubHeaderText>` followed by a `<Select>`—the trailing space is a visual separator, not sentence glue. This can be extracted as `t('datasetStats.historical.statsForColumn')` with the trailing space kept in the translation string, or the component can be refactored to use a `Trans` wrapper. — entity.profile.stats → t('historicalStats.statsForColumn')
- [x] 161:51 (extract) — `'Loading...'` — content prop of `<Message>` loading spinner — common.feedback → tc('loading')
- [x] 163:37 (extract) — `'Profiling Runs'` — section header title — entity.profile.stats → t('historicalStats.profilingRunsTitle')
- [x] 167:27 (extract) — `'Partition Stats'` — conditional section title when all profiles are partitioned — entity.profile.stats → t('historicalStats.partitionStats')
- [x] 167:57 (extract) — `'Table Stats'` — conditional section title when profiles are not partitioned — entity.profile.stats → t('historicalStats.tableStats')
- [x] 172:24 (extract) — `'Row Count Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.rowCountOverTime')
- [x] 179:24 (extract) — `'Column Count Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.columnCountOverTime')
- [x] 188:24 (extract) — `'Size Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.sizeOverTime')
- [x] 200:37 (extract) — `'Column Stats'` — section header title — entity.profile.stats → t('historicalStats.columnStats')
- [x] 204:24 (extract) — `'Null Count Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.nullCountOverTime')
- [x] 210:24 (extract) — `'Null Percentage Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.nullPercentageOverTime')
- [x] 219:24 (extract) — `'Distinct Count Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.distinctCountOverTime')
- [x] 225:24 (extract) — `'Distinct Percentage Over Time'` — chart title prop — entity.profile.stats → t('historicalStats.distinctPercentageOverTime')

## src/app/entityV2/shared/tabs/Dataset/Stats/historical/LookbackWindowSelect.tsx

- [x] 28:24 (extract) — `'Profiling history for past '` — prefix text prop passed to `PrefixedSelect`. Same trailing-space note as above; this is the rendered label shown to the user. Extract as `t('datasetStats.historical.profilingHistoryForPast')`. — entity.profile.stats → t('lookbackWindowSelect.profilingHistoryForPast')

## src/app/entityV2/shared/tabs/Dataset/Stats/historical/shared/PrefixedSelect.tsx

✓ no strings — `prefixText` is a prop; no hardcoded strings in this file

## src/app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows.ts

- [x] 7:34 (refactor) — `'1 hour'`, `'1 day'`, `'1 week'`, `'1 month'`, `'3 months'`, `'1 year'` — UI labels in a module-level constant object `LOOKBACK_WINDOWS`. These `text` properties are consumed by `LookbackWindowSelect` and `PrefixedSelect` as rendered option labels. Because they live in a module-level constant (not a component), they cannot call `t()` or `useTranslation()` directly. **Refactor approach (Pattern 3 / hook):** Replace the static `text` strings with stable keys (e.g. `'hour'`, `'day'`, `'week'`, `'month'`, `'quarter'`, `'year'`), create a `useLookbackWindowLabels()` hook that maps each key to `t('datasetStats.lookback.<key>')`, and derive the display label at the call site (`LookbackWindowSelect`) rather than from the constant. — entity.profile.stats → t('lookbackWindowOptions.hour'), t('lookbackWindowOptions.day'), t('lookbackWindowOptions.week'), t('lookbackWindowOptions.month'), t('lookbackWindowOptions.quarter'), t('lookbackWindowOptions.year') (use `useLookbackWindowLabels()` hook at call site)

## src/app/entityV2/shared/tabs/Dataset/Stats/snapshot/ColumnStats.tsx

- [x] 63:53 (extract) — `'unknown'` — placeholder rendered for null/undefined column stat cells — entity.profile.stats → t('columnStats.unknown')
- [x] 86:16 (extract) — `'Min'` — optional column title — entity.profile.stats → t('columnStats.minColumn')
- [x] 91:16 (extract) — `'Max'` — optional column title — entity.profile.stats → t('columnStats.maxColumn')
- [x] 96:16 (extract) — `'Mean'` — optional column title — entity.profile.stats → t('columnStats.meanColumn')
- [x] 101:16 (extract) — `'Median'` — optional column title — entity.profile.stats → t('columnStats.medianColumn')
- [x] 106:16 (extract) — `'Null Count'` — optional column title — entity.profile.stats → t('columnStats.nullCountColumn')
- [x] 111:16 (extract) — `'Null %'` — optional column title — entity.profile.stats → t('columnStats.nullPercentColumn')
- [x] 116:16 (extract) — `'Distinct Count'` — optional column title — entity.profile.stats → t('columnStats.distinctCountColumn')
- [x] 121:16 (extract) — `'Distinct %'` — optional column title — entity.profile.stats → t('columnStats.distinctPercentColumn')
- [x] 126:16 (extract) — `'Std. Dev'` — optional column title — entity.profile.stats → t('columnStats.stdDevColumn')
- [x] 131:16 (extract) — `'Sample Values'` — optional column title — entity.profile.stats → t('columnStats.sampleValuesColumn')
- [x] 148:16 (extract) — `'Name'` — required column title — entity.profile.stats → t('columnStats.nameColumn')
- [x] 172:22 (refactor) — `` `Column Stats for Partition ${partitionSpec.partition}` `` — conditional section title with interpolated partition value. **Refactor:** `t('datasetStats.snapshot.columnStatsForPartition', { partition: partitionSpec.partition })`. — entity.profile.stats → t('columnStats.columnStatsForPartition', { partition: partitionSpec.partition })
- [x] 172:72 (extract) — `'Column Stats'` — section title when not partitioned — entity.profile.stats → t('columnStats.columnStatsTitle')

## src/app/entityV2/shared/tabs/Dataset/Stats/snapshot/SampleValueTag.tsx

- [x] 32:28 (extract) — `'Copied'` — tooltip text shown after copying — entity.profile.stats → t('sampleValueTag.copied')
- [x] 32:39 (extract) — `'Click to copy'` — tooltip text before copying — entity.profile.stats → t('sampleValueTag.clickToCopy')

## src/app/entityV2/shared/tabs/Dataset/Stats/snapshot/TableStats.tsx

- [x] 47:46 (extract) — `'unknown'` — `lastReportedTimeString` fallback value used in tooltip text — entity.profile.stats → t('tableStats.unknown')
- [x] 66:22 (refactor) — `` `Partition Stats for Partition ${partitionSpec.partition}` `` — conditional section title with interpolation. **Refactor:** `t('datasetStats.snapshot.partitionStatsForPartition', { partition: partitionSpec.partition })`. — entity.profile.stats → t('tableStats.partitionStatsForPartition', { partition: partitionSpec.partition })
- [x] 66:71 (extract) — `'Table Stats'` — section title when not partitioned — entity.profile.stats → t('tableStats.tableStatsTitle')
- [x] 70:26 (extract) — `'Rows'` — `InfoItem` title prop — entity.profile.stats → t('tableStats.rowsLabel')
- [x] 79:26 (extract) — `'Columns'` — `InfoItem` title prop — entity.profile.stats → t('tableStats.columnsLabel')
- [x] 86:26 (extract) — `'Monthly Queries'` — `InfoItem` title prop — entity.profile.stats → t('tableStats.monthlyQueriesLabel')
- [x] 93:26 (extract) — `'Top Users'` — `InfoItem` title prop — entity.profile.stats → t('tableStats.topUsersLabel')
- [x] 110:26 (extract) — `'Last Updated'` — `InfoItem` title prop — entity.profile.stats → t('tableStats.lastUpdatedLabel')
- [x] 111:37 (refactor) — `` `Last reported at ${lastReportedTimeString}` `` — tooltip text with interpolated time. **Refactor:** `t('datasetStats.snapshot.lastReportedAt', { time: lastReportedTimeString })`. — entity.profile.stats → t('tableStats.lastReportedAt', { time: lastReportedTimeString })

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsHeader.tsx

- [x] 41:21 (extract) — `'Latest'` — button label text for the Latest view toggle — entity.profile.stats → t('statsHeader.latestButton')
- [x] 49:21 (extract) — `'Historical'` — button label text for the Historical view toggle — entity.profile.stats → t('statsHeader.historicalButton')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTab.tsx

- [x] 56:13 (refactor) — `` `Reported on ${toLocalDateString(...)} at ${toLocalTimeString(...)}` `` — `fullTableReportedAt` template literal with interpolated date and time. **Refactor:** `t('datasetStats.reportedAt', { date: toLocalDateString(...), time: toLocalTimeString(...) })`. — entity.profile.stats → t('statsTab.reportedAt', { date: toLocalDateString(...), time: toLocalTimeString(...) })
- [x] 61:13 (refactor) — same pattern for `partitionReportedAt` — same translation key and approach. — entity.profile.stats → t('statsTab.reportedAt', { date: toLocalDateString(...), time: toLocalTimeString(...) }) (same key)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable.tsx

- [x] 131:24 (extract) — `'No search results!'` — empty state message shown when search filter yields no columns — entity.profile.stats → t('columnStatsTable.noSearchResults')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable.utils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsV2.tsx

- [x] 46:24 (extract) — `'Column Stats'` — `PageTitle` `title` prop — entity.profile.stats → t('columnStatsV2.title')
- [x] 47:32 (extract) — `'View latest stats for each column in this table.'` — `PageTitle` `subTitle` prop — entity.profile.stats → t('columnStatsV2.subtitle')
- [x] 51:28 (extract) — `'Search Column Name'` — `SearchBar` `placeholder` prop — entity.profile.stats → t('columnStatsV2.searchPlaceholder')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/useGetColumnStatsColumns.tsx

- [x] 29:16 (extract) — `'Null Percentage'` — optional column title — entity.profile.stats → t('columnStatsTable.nullPercentageColumn')
- [x] 38:16 (extract) — `'Unique Values'` — optional column title — entity.profile.stats → t('columnStatsTable.uniqueValuesColumn')
- [x] 47:16 (extract) — `'Min'` — optional column title — entity.profile.stats → t('columnStatsTable.minColumn')
- [x] 55:16 (extract) — `'Max'` — optional column title — entity.profile.stats → t('columnStatsTable.maxColumn')
- [x] 70:16 (extract) — `'Column'` — required column title — entity.profile.stats → t('columnStatsTable.columnHeader')
- [x] 84:16 (extract) — `'Type'` — required column title — entity.profile.stats → t('columnStatsTable.typeColumn')
- [x] 119:20 (extract) — `'View'` — action button label inside the view column — common.actions → tc('view')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/ChangeHistoryGraph.tsx

- [x] 118:24 (extract) — `'Change History'` — `chartName` variable used as `GraphCard` `title` prop and analytics label — entity.profile.stats → t('changeHistoryGraph.title')
- [x] 133:55 (refactor) — `'change history'` — `statName` prop passed to `<NoPermission>`. This is a fragment used inside `NoPermission` to compose the message `"You do not have permission to view change history data."`. Since `NoPermission` interpolates it, it should be extracted at the `NoPermission` level (see that file's entry) and the callers should pass a translation key or translated string. — entity.profile.stats → t('changeHistoryGraph.statName') (pass translated string as prop; see `noPermission.message` key)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/ChangeHistoryDrawer.tsx

- [x] 94:16 (extract) — `'Change History Details'` — `Drawer` `title` prop — entity.profile.stats → t('changeHistoryDrawer.title')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/ChangeHistoryTimeline.tsx

- [x] 49:36 (extract) — `'There are no operations for the selected day'` — empty state message rendered inside `<Text>` — entity.profile.stats → t('changeHistoryTimeline.noOperations')
- [x] 70:29 (refactor) — `` `Truncated to show first ${numberOfOperations} ${pluralize(numberOfOperations, 'operation')}` `` — rendered inside `<Text>`. Contains dynamic count and pluralized noun. **Refactor:** use `Trans` component or `t('datasetStats.changeHistory.truncatedOperations', { count: numberOfOperations })` with `plural` key for the `pluralize` call. — entity.profile.stats → t('changeHistoryTimeline.truncatedOperations', { count: numberOfOperations }) (use i18next plural forms)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/ChangeTypePill.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/DateSwitcher.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineContent.tsx

- [x] 44:20 (skip) — `'by'` — grammatical connector before username; word order differs by language — wrap with eslint-disable/enable

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineDot.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineHeader.tsx

- [x] 43:30 (refactor) — `` `${abbreviateNumber(numberOfChanges)} ${pluralize(numberOfChanges, 'Change')}` `` — refactor to `t('timelineHeader.changesCount', { count: numberOfChanges, formattedCount: abbreviateNumber(numberOfChanges) })` using i18next plural forms (`_one`/`_other`); `count` drives pluralization, `formattedCount` is the display value — entity.profile.stats → t('timelineHeader.changesCount', { count: numberOfChanges, formattedCount: abbreviateNumber(numberOfChanges) })

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimeLineSkeleton.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/UsersSelect.tsx

- [x] 21:48 (extract) — `'Users'` — `selectLabelProps.label` prop rendered as the select's label — entity.profile.stats → t('usersSelect.label')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/constants.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useDebounceFalse.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetOperations.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUserName.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUsers.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUsers.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/usePrepareOperations.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useUsersSelectOptions.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/utils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryPopover.tsx

- [x] 98:16 (extract) — `'No data reported'` — empty state text rendered in `<Text>` when `!hasData` — entity.profile.stats → t('changeHistoryPopover.noData')
- [x] 107:16 (extract) — `'No changes this day'` — empty state text rendered in `<Text>` when `totalAmountOfOperations === 0` — entity.profile.stats → t('changeHistoryPopover.noChangesThisDay')
- [x] 139:32 (extract) — `'View Details'` — button label inside the popover — entity.profile.stats → t('changeHistoryPopover.viewDetails')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeTypeSummaryPill.tsx

✓ no strings — label is dynamically constructed from data (`operation.value` and `operation.name`)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/SelectSkeleton.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/Subtitle.tsx

- [x] 35:13 (extract) — `'Operations made to this table:'` — rendered inside `<span>` before the summary pills — entity.profile.stats → t('changeHistoryGraph.operationsMade')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/TypesSelect.tsx

- [x] 22:24 (extract) — `'Change Type'` — `placeholder` prop of `SimpleSelect` — entity.profile.stats → t('typesSelect.placeholder')
- [x] 23:49 (extract) — `'Change Type'` — `selectLabelProps.label` prop (same string, but a separate prop) — entity.profile.stats → t('typesSelect.label') (can share same value as placeholder)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/constants.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useChangeHistoryData.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useColorAccessors.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useDataRange.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useGetCalendarRangeByTimeRange.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils.ts

- [x] 71:18 (refactor) — `'Insert'`, `'Update'`, `'Delete'`, `'Alter'`, `'Create'`, `'Drop'` — operation `name` fields in the module-level `convertAggregationsToOperationsData` function. These strings are used as display names for chart pills, popover rows, and table headers. Because they are constructed inside a plain function (not a hook or component), they cannot call `t()` directly. **Refactor approach (Pattern 2 — getter function):** Replace the inline string literals with stable programmatic keys (already available from `OperationType` enum values), add a standalone `getOperationDisplayName(key: OperationType, t: TFunction): string` helper, and call it at render time in `ChangeTypeSummaryPill`, `ChangeHistoryPopover`, and related display components. — entity.profile.stats → t('changeHistoryGraph.operation.insert'), t('changeHistoryGraph.operation.update'), t('changeHistoryGraph.operation.delete'), t('changeHistoryGraph.operation.alter'), t('changeHistoryGraph.operation.create'), t('changeHistoryGraph.operation.drop') (expose via `getOperationDisplayName(key, t)` helper called at render time)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/GraphPopover.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MonthOverMonthPill.tsx

- [x] 19:37 (refactor) — `` `${value}% MoM` `` — `Pill` `label` prop with interpolated percentage. **Note:** this component is currently disabled (`IS_MOM_PILL_DISABLED = true`) and always returns `null`. Extract anyway to be future-ready: `t('datasetStats.graphs.momPill', { value })`. — entity.profile.stats → t('monthOverMonthPill.label', { value })

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent.tsx

- [x] 25:22 (refactor) — `` `Turn on profiling for the ${platformName} source in order to see stats here.` `` — rendered `<Text>` with interpolated platform name. **Refactor:** `t('datasetStats.moreInfo.turnOnProfiling', { platformName })`. — entity.profile.stats → t('moreInfoModal.turnOnProfiling', { platformName })
- [x] 27:20 (refactor) — `'You can '` + `<a>view documentation</a>` + `' for more help.'` — `<Text>` with inline anchor element. This requires a `Trans` component: `<Trans i18nKey="datasetStats.moreInfo.viewDocumentation">You can <a href={...}>view documentation</a> for more help.</Trans>`. — entity.profile.stats → t('moreInfoModal.viewDocumentation') via `<Trans i18nKey="entity.stats:moreInfoModal.viewDocumentation">You can <a href={...}>view documentation</a> for more help.</Trans>`

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect.tsx

- [x] 29:24 (extract) — `'Choose time range'` — `placeholder` prop of `SimpleSelect` — entity.profile.stats → t('timeRangeSelect.placeholder')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants.tsx

- [x] 13:45 (refactor) — `'Last Week'`, `'Last 30 Days'`, `'Last 3 Months'`, `'Last 6 Months'`, `'Last Year'` — `text` fields of `GRAPH_LOOKBACK_WINDOWS` constant object, and label strings in `getTimeRangeLabel`. These are module-level constants used as rendered option labels in `TimeRangeSelect`. **Refactor approach (Pattern 2):** Convert `GRAPH_LOOKBACK_WINDOWS` entries and `getTimeRangeLabel` to accept a `t` function parameter (or provide a `useGraphLookbackWindowLabels()` hook), replacing the hardcoded English strings with `t('datasetStats.graphs.lookback.<key>')` calls at render time. — entity.profile.stats → t('lookbackWindowOptions.lastWeek'), t('lookbackWindowOptions.last30Days'), t('lookbackWindowOptions.last3Months'), t('lookbackWindowOptions.last6Months'), t('lookbackWindowOptions.lastYear') (use hook or pass `t` at render site)
- [x] 29:16 (refactor) — `'Last Week'` — in `getTimeRangeLabel` switch — entity.profile.stats → t('lookbackWindowOptions.lastWeek')
- [x] 31:16 (refactor) — `'Last 30 days'` — in `getTimeRangeLabel` switch — entity.profile.stats → t('lookbackWindowOptions.last30Days')
- [x] 33:16 (refactor) — `'Last 3 months'` — in `getTimeRangeLabel` switch — entity.profile.stats → t('lookbackWindowOptions.last3Months')
- [x] 35:16 (refactor) — `'Last 6 months'` — in `getTimeRangeLabel` switch — entity.profile.stats → t('lookbackWindowOptions.last6Months')
- [x] 37:16 (refactor) — `'Last Year'` — in `getTimeRangeLabel` switch — entity.profile.stats → t('lookbackWindowOptions.lastYear')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByTimeRange.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptions.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeseriesCapabilities.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission.tsx

- [x] 20:22 (extract) — `'No Permission'` — heading text rendered in `<Text size="2xl" weight="bold">` — entity.profile.stats → t('noPermission.heading')
- [x] 21:21 (refactor) — `` `You do not have permission to view ${statName} data.` `` — body text with interpolated `statName` prop. **Refactor:** `t('datasetStats.noPermission.message', { statName })`. All callers (`ChangeHistoryGraph`, `QueryCountChart`, `StorageSizeGraph`, `TopUsers`) pass hardcoded English strings as `statName`; those strings should also be extracted (see each file's entry) or replaced with translation keys for the interpolation to be correct in all locales. — entity.profile.stats → t('noPermission.message', { statName }) (callers supply translated `statName` via their own keys)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/QueryCountGraph/QueryCountChart.tsx

- [x] 76:24 (extract) — `'Daily Query Count'` — `chartName` variable used as `GraphCard` `title` and analytics label — entity.profile.stats → t('queryCountChart.title')
- [x] 96:57 (refactor) — `'daily query count'` — `statName` prop passed to `<NoPermission>`. See `NoPermission` entry; extract at caller as `t('datasetStats.queryCount.statName')` and pass as prop, or pass the translated string directly. — entity.profile.stats → t('queryCountChart.statName') (pass translated string as prop)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/QueryCountGraph/useQueryCountData.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StatsTabRowCountGraph/StatsTabRowCountGraph.tsx

- [x] 17:34 (extract) — `'Row Count'` — `DEFAULT_GRAPH_NAME` constant used as `chartName` passed to `TimeRangeSelect` analytics and `RowCountGraph` title prop — entity.profile.stats → t('rowCountGraph.title')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StorageSizeGraph/StorageSizeGraph.tsx

- [x] 61:24 (extract) — `'Storage Size'` — `chartName` variable used as `GraphCard` `title` and analytics label — entity.profile.stats → t('storageSizeGraph.title')
- [x] 68:57 (refactor) — `'storage size'` — `statName` prop passed to `<NoPermission>`. Extract at caller as `t('datasetStats.storageSize.statName')`. — entity.profile.stats → t('storageSizeGraph.statName') (pass translated string as prop)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StorageSizeGraph/useStorageSizeData.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils.ts

✓ no strings — date format strings (`'DD MMM'`, `'MMM YY'`, etc.) are dayjs format tokens, not user-visible text

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/LastMonthStats.tsx

- [x] 26:14 (extract) — `'Last 30 days'` — section label rendered in `<Text size="sm" weight="bold">` — entity.profile.stats → t('lastMonthStats.label')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/LatestStats.tsx

- [x] 29:14 (extract) — `'Latest'` — section label rendered in `<Text size="sm" weight="bold">` — entity.profile.stats → t('latestStats.label')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/SelectSiblingDropdown.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/StatsHighlights.tsx

- [x] 30:28 (extract) — `'Highlights'` — `PageTitle` `title` prop — entity.profile.stats → t('statsHighlights.title')
- [x] 31:35 (extract) — `'View the latest statistics for this table'` — `PageTitle` `subTitle` prop — entity.profile.stats → t('statsHighlights.subtitle')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/styledComponents.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/useGetSiblingsOptions.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/ViewButton.tsx

- [x] 8:9 (extract) — `'View'` — `Button` children text — common.actions → tc('view')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/HistoricalSectionHeader.tsx

- [x] 8:24 (extract) — `'Historical'` — `PageTitle` `title` prop — entity.profile.stats → t('historicalSection.title')
- [x] 9:32 (extract) — `'View important trends for this table'` — `PageTitle` `subTitle` prop — entity.profile.stats → t('historicalSection.subtitle')

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/RowsAndUsers.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/TopUsers.tsx

- [x] 65:16 (extract) — `'Name'` — column title for top users table — entity.profile.stats → t('topUsers.nameColumn')
- [x] 88:16 (extract) — `'Queries last month'` — column title for top users table — entity.profile.stats → t('topUsers.queriesLastMonthColumn')
- [x] 107:24 (extract) — `'Top Users'` — `GraphCard` `title` prop — entity.profile.stats → t('topUsers.title')
- [x] 110:57 (refactor) — `'top users'` — `statName` prop passed to `<NoPermission>`. Extract at caller as `t('datasetStats.topUsers.statName')`. — entity.profile.stats → t('topUsers.statName') (pass translated string as prop)

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSections.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsTabV2.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsSections.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabWrapper.tsx

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Stats/viewType.ts

✓ no strings

## src/app/entityV2/shared/tabs/Dataset/Timeline/SchemaTimelineSection.tsx

- [x] 59:22 (skip) — `'subtitle'` — hardcoded placeholder string assigned to `entry.subtitle` but never rendered in JSX (no reference to `t.subtitle` in the render output). Skip: internal stub, not user-visible.
- [x] 60:18 (skip) — `'description'` — same: `entry.desc` is never rendered in JSX. Skip.

## src/app/entityV2/shared/tabs/Dataset/View/ViewDefinitionTab.tsx

- [x] 83:37 (refactor) — `['Source', 'Compiled']` — `formatOptions` array entries used as `Radio.Group` option labels when the dataset is from dbt. **Refactor:** `[t('datasetStats.view.source'), t('datasetStats.view.compiled')]`. — entity.profile.view → t('viewDefinitionTab.formatSource'), t('viewDefinitionTab.formatCompiled')
- [x] 83:48 (refactor) — `['Raw', 'Formatted']` — `formatOptions` array entries used as `Radio.Group` option labels for non-dbt datasets. **Refactor:** `[t('datasetStats.view.raw'), t('datasetStats.view.formatted')]`. — entity.profile.view → t('viewDefinitionTab.formatRaw'), t('viewDefinitionTab.formatFormatted')
- [x] 89:37 (extract) — `'Details'` — `Typography.Title` section heading — entity.profile.view → t('viewDefinitionTab.detailsHeading')
- [x] 91:28 (extract) — `'Materialized'` — `InfoItem` `title` prop — entity.profile.view → t('viewDefinitionTab.materializedLabel')
- [x] 93:51 (extract) — `'True'` — rendered value when `materialized === true` — entity.profile.view → t('viewDefinitionTab.materializedTrue')
- [x] 93:59 (extract) — `'False'` — rendered value when `materialized === false` — entity.profile.view → t('viewDefinitionTab.materializedFalse')
- [x] 95:28 (extract) — `'Language'` — `InfoItem` `title` prop — entity.profile.view → t('viewDefinitionTab.languageLabel')
- [x] 100:37 (extract) — `'Logic'` — `Typography.Title` section heading — entity.profile.view → t('viewDefinitionTab.logicHeading')
