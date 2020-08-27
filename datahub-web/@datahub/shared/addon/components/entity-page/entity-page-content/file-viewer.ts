// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/entity-page-content/file-viewer';
import { layout } from '@ember-decorators/component';
import { IFileViewerComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import BasePageComponent from '@datahub/shared/components/entity-page/base-page-component';

/**
 * Component that will use ember-ace to display the content of a file
 */
@layout(template)
export default class EntityPageContentFileViewer<E> extends BasePageComponent {
  /**
   * See documentation in the interface IFileViewerComponent.
   */
  @assertComponentPropertyNotUndefined
  options!: IFileViewerComponent<E>['options'];
}
