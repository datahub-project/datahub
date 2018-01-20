import Controller from '@ember/controller';

export default Controller.extend({
  actions: {
    onSelect: function(dataset, data) {
      highlightRow(dataset, data, false);
      if (dataset && dataset.id != 0) {
        updateTimeLine(dataset.id, false);
      }
    }
  },

  genBreadcrumbs(urn) {
    var breadcrumbs = [];
    var b = urn.split('/');
    b.shift();
    for (var i = 0; i < b.length; i++) {
      var updatedUrn = b[i];
      if (i === 0) {
        breadcrumbs.push({ title: b[i], urn: updatedUrn });
      } else {
        breadcrumbs.push({ title: b[i], urn: updatedUrn });
      }
    }
    return breadcrumbs;
  }
});
