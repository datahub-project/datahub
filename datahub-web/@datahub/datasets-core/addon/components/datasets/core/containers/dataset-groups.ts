import { IDatasetGroupsAPIResponse, IDatasetGroupAPIResponse } from '@datahub/datasets-core/types/groups';
import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../../templates/components/datasets/core/containers/dataset-groups';
import { layout, classNames } from '@ember-decorators/component';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { task } from 'ember-concurrency';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { set, computed } from '@ember/object';
import { readDatasetGroups } from '@datahub/data-models/api/dataset/groups';
import { datasetGroupNameRegexPattern } from '@datahub/data-models/constants/entity/dataset/groups';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component.d';

const baseTableClass = 'dataset-groups-table';
const tableTitle = 'Groups';

/**
 * The type of each DatasetGroup created solely from the `groupsResponse` so that we can display it as per our liking in the UI
 */
interface IDatasetGroupMap {
  // Name of the dataset group. Ex: OauthDatasets
  name: string;
  // The namespace that precedes the name and provides more context. Ex : com.abc.apidatasets
  namespace: string;
}

@layout(template)
@classNames('dataset-groups__container')
@containerDataSource<DatasetGroupsContainer>('getContainerDataTask', ['urn'])
export default class DatasetGroupsContainer extends Component {
  /**
   * Styling class used in the template
   */
  baseTableClass = baseTableClass;

  /**
   * Title of the table that holds the groups' info
   */
  tableTitle = tableTitle;

  /**
   * The urn is a passed in element that is used by this container to know the context of the entity we are
   * displaying as an entity is identified by this unique urn key
   */
  urn = '';

  /**
   * Response data for the dataset groups API call
   */
  groupsResponse: IDatasetGroupsAPIResponse = [];

  /**
   * Method responsible for return the a Dynamic Component that can be used as a header for the groups table
   */
  get dynamicHeaderComponent(): IDynamicComponent {
    return {
      name: 'dynamic-components/header',
      options: {
        className: `${baseTableClass}__dynamic-header`,
        title:
          'A group is a subset of related datasets. Each group has a namespace and a description associated with it',
        contentComponents: [
          {
            name: 'dynamic-components/composed/user-assistance/help-tooltip-with-link',
            options: {
              className: `${baseTableClass}__tooltip`,
              text:
                'Standardized identifier used for GDPR purposes to identify and group a static collection of datasets. Makes purging and onboarding of subsets to dashboards easier'
            }
          }
        ]
      }
    };
  }

  /**
   * Local state representative of the groups that the dataset belongs to.
   * Groups is formed by extracting a list of names from the `groupsResponse` property
   *
   * We construct the `DatasetGroupMap` for each group available by calling the `formatDatasetGroupNames` helper function,
   * and make a list called `groupList`
   *
   */
  @computed('groupsResponse')
  get groups(): Array<IDatasetGroupMap> {
    return this.groupsResponse.reduce(
      (groupList: Array<IDatasetGroupMap>, currentGroup: IDatasetGroupAPIResponse): Array<IDatasetGroupMap> => {
        const groupNameMatches = datasetGroupNameRegexPattern.exec(currentGroup.urn);
        if (groupNameMatches) {
          return [...groupList, this.formatDatasetGroupNames(groupNameMatches)];
        }
        return groupList;
      },
      []
    );
  }

  /**
   * Helper method to parse the input into a usable object.
   * We construct both `namespace` and `name` as part of this object
   *
   * @param groupNameMatches The incoming param containing the groupNames
   */
  formatDatasetGroupNames(groupNameMatches: Array<string>): IDatasetGroupMap {
    const groupNames = groupNameMatches[1].split(',');
    return {
      namespace: groupNames[0],
      name: groupNames[1]
    };
  }

  /**
   * Text displayed at the template level when there are no groups associated with the dataset
   */
  emptyStateDisplayText = 'There are no groups that this dataset belongs to';

  /**
   * A boolean flag to indicate if there are groups or not
   */
  @computed('groups')
  get isEmpty(): boolean {
    return !this.groups.length;
  }

  /**
   * Async task that reads the dataset groups from `readDatasetGroups`,
   * then extracts the datasetName from it ( if any )

   * Also responsible for setting the value for `groupsResponse`
   */
  @(task(function*(this: DatasetGroupsContainer): IterableIterator<Promise<IDatasetGroupsAPIResponse>> {
    const { urn } = this;
    const emptyGroups: IDatasetGroupsAPIResponse = [];

    if (urn) {
      const groupsResponse = yield readDatasetGroups(urn);
      set(this, 'groupsResponse', groupsResponse || emptyGroups);
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<IDatasetGroupsAPIResponse>;
}
