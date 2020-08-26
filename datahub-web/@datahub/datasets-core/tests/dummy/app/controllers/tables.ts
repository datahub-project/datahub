import Controller from '@ember/controller';
import { action } from '@ember/object';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import DatasetComplianceInfo from '@datahub/data-models/entity/dataset/modules/compliance-info';
import { set } from '@ember/object';

export default class TablesController extends Controller {
  model!: {
    complianceInfo: DatasetComplianceInfo;
  };

  isEditing = false;

  @action
  onCancel(): void {
    this.model.complianceInfo.createWorkingCopy();
    set(this, 'isEditing', false);
  }

  @action
  addToAnnotations(workingTag: DatasetComplianceAnnotation): void {
    const { model } = this;

    if (model) {
      model.complianceInfo.addAnnotation(workingTag);
    }
  }

  @action
  removeFromAnnotations(workingTag: DatasetComplianceAnnotation): void {
    const { model } = this;

    if (model) {
      model.complianceInfo.removeAnnotation(workingTag);
    }
  }

  @action
  saveCompliance(workingTags: Array<DatasetComplianceAnnotation>): void {
    set(this.model.complianceInfo, 'annotations', workingTags);
  }

  @action
  resetWorkingCopy(): void {
    this.model.complianceInfo.createWorkingCopy();
  }
}
