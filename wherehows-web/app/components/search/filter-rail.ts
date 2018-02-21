import Component from '@ember/component';

export default class extends Component {
  tagName = 'ul';

  classNames = ['nacho-filter-rail'];

  checkboxComponent = 'search/checkbox-group';

  radioGroupComponent = 'search/radio-group';

  dropdownComponent = 'search/dropdown-selection';

  dateRangeComponent = 'search/daterange-selection';

  linksComponent = 'search/link-group';
}
