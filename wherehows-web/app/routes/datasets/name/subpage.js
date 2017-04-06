import Ember from 'ember';

export default Ember.Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'datasets',

  setupController: function (controller, param) {
    if (!this.controller)
      return;

    var listUrl = 'api/v1/list/datasets?urn=' + param.urn;
    var addSlash = false;
    if (param.urn && (param.urn[param.urn.length - 1] != '/')) {
      addSlash = true;
    }
    if (addSlash) {
      listUrl += '/';
    }
    $.get(listUrl, function (data) {
      if (data && data.status == "ok") {
        // renderDatasetListView(data.nodes);
      }
    });
    var url = 'api/v1/datasets?size=10&page=' + param.page + '&urn=' + param.urn;
    if (addSlash) {
      url += '/';
    }
    currentTab = 'Datasets';
    updateActiveTab();
    var breadcrumbs = [{"title": "DATASETS_ROOT", "urn": "page/1"}];
    var urn = param.urn.replace("://", "");
    var b = urn.split('/');
    for (var i = 0; i < b.length; i++) {
      if (i === 0) {
        breadcrumbs.push({
          title: b[i],
          urn: "name/" + b[i] + "/page/1?urn=" + b[i] + ':///'
        })
      } else {
        var urn = b.slice(0, (i + 1)).join('/');
        breadcrumbs.push({
          title: b[i],
          urn: "name/" + b[i] + "/page/1?urn=" + param.urn.split('/').splice(0, i + 3).join('/')
        })
      }
    }
    var watcherEndpoint = "/api/v1/urn/watch?urn=" + param.urn;
    if (addSlash) {
      watcherEndpoint += '/';
    }
    $.get(watcherEndpoint, data => {
      if (data.id && data.id !== 0) {
        this.controller.set('urnWatched', true)
        this.controller.set('urnWatchedId', data.id)
      } else {
        this.controller.set('urnWatched', false)
        this.controller.set('urnWatchedId', 0)
      }
    });

    if (this.controller.currentName && param.urn) {
      // findAndActiveDatasetNode(this.controller.currentName, param.urn);
    }

    $.get(url, data => {
      if (data && data.status == "ok") {
        this.controller.set('model', data);
        this.controller.set('urn', param.urn);
        this.controller.set('breadcrumbs', breadcrumbs);
        this.controller.set('detailview', false);
      }
    });
  },
  actions: {
    getDatasets: function () {
      var url = 'api/v1/datasets?size=10&page=' +
          this.controller.get('model.data.page') +
          '&urn=' +
          this.controller.get('urn');
      currentTab = 'Datasets';
      updateActiveTab();
      $.get(url, data => {
        if (data && data.status == "ok") {
          this.controller.set('model', data);
          this.controller.set('urn', this.controller.get('urn'));
          this.controller.set('detailview', false);
        }
      });
    }
  }
});
