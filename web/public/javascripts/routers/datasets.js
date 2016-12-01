
function initializeDependsTreeGrid()
{
  $('#depends-table').treegrid();
}

function initializeReferencesTreeGrid()
{
  $('#references-table').treegrid();
}

function formatValue(key, value){
  switch(key) {
    case 'modification_time':
    case 'begin_date':
    case 'lumos_process_time':
    case 'end_date':
    case 'oracle_time':
      if(value < 0 )
        return value
      var obj = value;
      try
      {
        obj = new Date(value).toISOString();
      }
      catch(err)
      {
        console.log("Invalid date for " + key + " : " + value);
      }
      return obj;
      break;
    case 'dumpdate':
      var y = value.substring(0,4)
      var mm = value.substring(4,6)
      var d= value.substring(6,8)
      var h = value.substring(8,10)
      var m = value.substring(10,12)
      var s = value.substring(12,14)
      return y + '-' + mm + '-' + d + ' ' + h + ':' + m + ':' + s
      break;
    default:
      return value;
  }
}

function convertPropertiesToArray(properties)
{
  var propertyArray = [];
  if (properties)
    {
        for (var key in properties)
        {
          if ((key.toLowerCase() != 'elements') && (key.toLowerCase() != 'view_depends_on'))
          {
            var isSelectController = false;
            if (key.toLowerCase() == 'view_expanded_text' || key.toLowerCase() == 'viewSqlText')
            {
              isSelectController = true;
            }
            else
            {
              isSelectController = false;
            }
            if (typeof properties[key] !== 'object')
            {
              if (key == 'connectionURL' && properties[key])
              {
                var list = properties[key].split(',');
                if (list && list.length > 0)
                {
                  propertyArray.push({'isSelectController': isSelectController,
                    'key':key, 'value':JsonHuman.format(list)});
                }
                else
                {
                  propertyArray.push({'isSelectController': isSelectController,
                    'key':key, 'value': properties[key]});
                }
              }
              else
              {
                var value = formatValue(key, properties[key]);
                if (!value && !(value === 0))
                  value = 'NULL';
                propertyArray.push({'isSelectController': isSelectController,
                  'key':key, 'value': value});
              }
            }
            else
            {
              propertyArray.push({'isSelectController': isSelectController,
                'key':key, 'value':JsonHuman.format(properties[key])});
            }

          }
        }
    }
    return propertyArray;
}

var datasetController = null;
App.DatasetsRoute = Ember.Route.extend({
  setupController: function(controller) {
    datasetController = controller;
  },
  actions: {
    getDatasets: function(){
      var listUrl = 'api/v1/list/datasets';
      $.get(listUrl, function(data) {
        if (data && data.status == "ok"){
          renderDatasetListView(data.nodes);
        }
      });
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Datasets';
      updateActiveTab();
      $.get(url, function(data) {
        if (data && data.status == "ok"){
          datasetController.set('model', data);
          datasetController.set('urn', null);
          datasetController.set('detailview', false);
        }
      });
    }
  }
});

App.DatasetRoute = Ember.Route.extend({
  setupController(controller, params) {
    currentTab = 'Datasets';
    updateActiveTab();
    var id = 0;
    var source = '';
    var urn = '';
    var name = '';
    controller.set("hasProperty", false);

    if (params && params.id) {
      ({id, source, urn, name} = params);
      let {originalSchema = null} = params;

      datasetController.set('detailview', true);
      if (originalSchema) {
        Ember.set(params, 'schema', originalSchema);
      }

      controller.set('model', params);
    } else if (params.dataset) {
      ({id, source, urn, name} = params.dataset);

      controller.set('model', params.dataset);
      datasetController.set('detailview', true);
    }

    // Don't set default zero Ids on controller
    if (id) {
      controller.set('datasetId', id);
    }

      var instanceUrl = 'api/v1/datasets/' + id + "/instances";
      $.get(instanceUrl, function(data) {
        if (data && data.status == "ok" && data.instances && data.instances.length > 0) {
          controller.set("hasinstances", true);
          controller.set("instances", data.instances);
          controller.set("currentInstance", data.instances[0]);
          controller.set("latestInstance", data.instances[0]);
          var versionUrl = 'api/v1/datasets/' + id + "/versions/db/" + data.instances[0].dbId;
          $.get(versionUrl, function(data) {
            if (data && data.status == "ok" && data.versions && data.versions.length > 0) {
              controller.set("hasversions", true);
              controller.set("versions", data.versions);
              controller.set("currentVersion", data.versions[0]);
              controller.set("latestVersion", data.versions[0]);
            }
            else
            {
              controller.set("hasversions", false);
              controller.set("currentVersion", '0');
              controller.set("latestVersion", '0');
            }
          });
        }
        else
        {
          controller.set("hasinstances", false);
          controller.set("currentInstance", '0');
          controller.set("latestInstance", '0');
          controller.set("hasversions", false);
          controller.set("currentVersion", '0');
          controller.set("latestVersion", '0');
        }
      });

      if (urn)
      {
        var index = urn.lastIndexOf('/');
        if (index != -1)
        {
          var listUrl = 'api/v1/list/datasets?urn=' + urn.substring(0, index+1);
          $.get(listUrl, function(data) {
            if (data && data.status == "ok"){
              renderDatasetListView(data.nodes, name);
            }
          });
        }
      }

      if (datasetCommentsComponent)
      {
        datasetCommentsComponent.getComments();
      }

      if (urn)
        {
          urn = urn.replace('<b>', '').replace('</b>', '');
          var index = urn.lastIndexOf("/");
          if (index != -1)
          {
            var name = urn.substring(index +1);
            findAndActiveDatasetNode(name, urn);
          }
          var breadcrumbs = [{"title":"DATASETS_ROOT", "urn":"page/1"}];
          var updatedUrn = urn.replace("://", "");
          var b = updatedUrn.split('/');
          for(var i = 0; i < b.length; i++) {
            if( i === 0) {
              breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + b[i] + ':///'
              })
            }
            else if (i === (b.length -1))
            {
              breadcrumbs.push({
                title: b[i],
                urn: id
              })
            }
            else {
              breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + urn.split('/').splice(0, i+3).join('/')
              })
            }
          }
          controller.set("breadcrumbs", breadcrumbs);
        }

        var ownerTypeUrl = 'api/v1/owner/types';
        $.get(ownerTypeUrl, function(data) {
          if (data && data.status == "ok") {
            controller.set("ownerTypes", data.ownerTypes);
          }
        });

        var userSettingsUrl = 'api/v1/user/me';
        $.get(userSettingsUrl, function(data) {
          var tabview = false;
          if (data && data.status == "ok")
          {
            controller.set("currentUser", data.user);
            if (data.user && data.user.userSetting && data.user.userSetting.detailDefaultView)
            {
              if (data.user.userSetting.detailDefaultView == 'tab')
              {
                tabview = true;
              }
            }
          }
          controller.set("tabview", tabview);
        });

        var columnUrl = 'api/v1/datasets/' + id + "/columns";
        $.get(columnUrl, function(data) {
          if (data && data.status == "ok")
            {
              if (data.columns && (data.columns.length > 0))
                {
                  controller.set("hasSchemas", true);

                  data.columns = data.columns.map(function(item, idx){
                    if (item.comment)
                    {
                      item.commentHtml = marked(item.comment).htmlSafe()
                    }
                    return item
                  })
                  controller.set("schemas", data.columns);
                  controller.buildJsonView();
                  setTimeout(initializeColumnTreeGrid, 500);
                }
                else
                  {
                    controller.set("hasSchemas", false);
                    controller.set("schemas", null);
                    controller.buildJsonView();
                  }
            }
            else
              {
                controller.set("hasSchemas", false);
                controller.set("schemas", null);
                controller.buildJsonView();
              }
        });

        if (source.toLowerCase() != 'pinot')
          {
            propertiesUrl = 'api/v1/datasets/' + id + "/properties";
            $.get(propertiesUrl, function(data) {
              if (data && data.status == "ok")
                {
                  if (data.properties)
                    {
                      var propertyArray = convertPropertiesToArray(data.properties);
                      if (propertyArray && propertyArray.length > 0)
                        {
                          controller.set("hasProperty", true);
                          controller.set("properties", propertyArray);
                        }
                        else
                          {
                            controller.set("hasProperty", false);
                          }
                    }
                }
                else
                  {
                    controller.set("hasProperty", false);
                  }
            });
          }

          var sampleUrl;
          if (source.toLowerCase() == 'pinot')
            {
              sampleUrl = 'api/v1/datasets/' + id + "/properties";
              $.get(sampleUrl, function(data) {
                if (data && data.status == "ok")
                  {
                    if (data.properties && data.properties.elements && (data.properties.elements.length > 0)
                        && data.properties.elements[0] && data.properties.elements[0].columnNames
                      && (data.properties.elements[0].columnNames.length > 0))
                      {
                        controller.set("hasSamples", true);
                        controller.set("samples", data.properties.elements[0].results);
                        controller.set("columns", data.properties.elements[0].columnNames);
                      }
                      else
                        {
                          controller.set("hasSamples", false);
                        }
                  }
                  else
                    {
                      controller.set("hasSamples", false);
                    }
              });
            }
              else
                {
                  sampleUrl = 'api/v1/datasets/' + id + "/sample";
                  $.get(sampleUrl, function(data) {
                    if (data && data.status == "ok")
                      {
                        if (data.sampleData && data.sampleData.sample && (data.sampleData.sample.length > 0))
                          {
                            controller.set("hasSamples", true);
                            var tmp = {};
                            var count = data.sampleData.sample.length
                            var d = data.sampleData.sample
                            for(var i = 0; i < count; i++) {
                              tmp['record ' + i] = d[i]
                            }
                            var node = JsonHuman.format(tmp)
                            setTimeout(function(){
                              var json_human = document.getElementById('datasetSampleData-json-human');
                              if (json_human)
                              {
                                if (json_human.children && json_human.children.length > 0)
                                {
                                  json_human.removeChild(json_human.childNodes[json_human.children.length-1]);
                                }

                                json_human.appendChild(node)
                              }

                            }, 500);
                          }
                          else
                            {
                              controller.set("hasSamples", false);
                            }
                      }
                      else
                        {
                          controller.set("hasSamples", false);
                        }
                  });
                }

                var impactAnalysisUrl = 'api/v1/datasets/' + id + "/impacts";
                $.get(impactAnalysisUrl, function(data) {
                  if (data && data.status == "ok")
                    {
                      if (data.impacts && (data.impacts.length > 0))
                        {
                          controller.set("hasImpacts", true);
                          controller.set("impacts", data.impacts);
                        }
                        else
                          {
                            controller.set("hasImpacts", false);
                          }
                    }
                });

    var datasetDependsUrl = 'api/v1/datasets/' + id + "/depends";
    $.get(datasetDependsUrl, function(data) {
      if (data && data.status == "ok")
      {
        if (data.depends && (data.depends.length > 0))
        {
          controller.set("hasDepends", true);
          controller.set("depends", data.depends);
          setTimeout(initializeDependsTreeGrid, 500);
        }
        else
        {
          controller.set("hasDepends", false);
        }
      }
    });

    var datasetPartitionsUrl = 'api/v1/datasets/' + id + "/access";
    $.get(datasetPartitionsUrl, function(data) {
      if (data && data.status == "ok")
      {
        if (data.access && (data.access.length > 0))
        {
          controller.set("hasAccess", true);
          controller.set("accessibilities", data.access);
        }
        else
        {
          controller.set("hasAccess", false);
        }
      }
    });

    var datasetReferencesUrl = 'api/v1/datasets/' + id + "/references";
    $.get(datasetReferencesUrl, function(data) {
      if (data && data.status == "ok")
      {
        if (data.references && (data.references.length > 0))
        {
          controller.set("hasReferences", true);
          controller.set("references", data.references);
          setTimeout(initializeReferencesTreeGrid, 500);
        }
        else
        {
          controller.set("hasReferences", false);
        }
      }
    });

    Promise.resolve($.get(`api/v1/datasets/${id}/owners`))
        .then(({status, owners = []}) => {
          status === 'ok' && controller.set('owners', owners);
        })
        .then($.get.bind($, 'api/v1/party/entities'))
        .then(({status, userEntities = []}) => {
          if (status === 'ok' && userEntities.length) {
            /**
             * @type {Object} userEntitiesMaps hash of userEntities: label -> displayName
             */
            const userEntitiesMaps = userEntities.reduce((map, {label, displayName}) =>
                (map[label] = displayName, map), {});

            controller.setProperties({
              userEntitiesSource: Object.keys(userEntitiesMaps),
              userEntitiesMaps
            });
          }
        });
  },

  model({id}) {
    return Ember.$.getJSON(`api/v1/datasets/${id}`);
  },

  actions: {
    getSchema: function(){
      var controller = this.get('controller')
      var id = this.get('controller.model.id')
      var columnUrl = 'api/v1/datasets/' + id + "/columns";
      controller.set("isTable", true);
      controller.set("isJSON", false);
      $.get(columnUrl, function(data) {
        if (data && data.status == "ok")
          {
            if (data.columns && (data.columns.length > 0))
              {
                controller.set("hasSchemas", true);
                data.columns = data.columns.map(function(item, idx){
                  item.commentHtml = marked(item.comment).htmlSafe()
                  return item
                })
                controller.set("schemas", data.columns);
                setTimeout(initializeColumnTreeGrid, 500);
              }
              else
                {
                  controller.set("hasSchemas", false);
                }
          }
          else
            {
              controller.set("hasSchemas", false);
            }
      });
    },
    getDataset: function(){
      var _this = this
      $.get
      ( '/api/v1/datasets/' + this.get('controller.model.id')
          , function(data){
            if(data.status == "ok") {
              _this.set('controller.model', data.dataset)
            }
          }
      )
    }
  }
});

App.NameRoute = Ember.Route.extend({
  setupController: function(controller, param) {
    datasetController.set('currentName', param.name);
  },
  actions: {
    getDatasets: function(){
      var listUrl = 'api/v1/list/datasets';
      $.get(listUrl, function(data) {
        if (data && data.status == "ok"){
          renderDatasetListView(data.nodes);
        }
      });
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Datasets';
      updateActiveTab();
      $.get(url, function(data) {
        if (data && data.status == "ok"){
          datasetController.set('model', data);
          datasetController.set('urn', null);
          datasetController.set('detailview', false);
        }
      });
    }
  }
});

App.PageRoute = Ember.Route.extend({
  setupController: function(controller, param) {
    var listUrl = 'api/v1/list/datasets';
    $.get(listUrl, function(data) {
      if (data && data.status == "ok"){
        renderDatasetListView(data.nodes);
      }
    });
    var url = 'api/v1/datasets?size=10&page=' + param.page;
    currentTab = 'Datasets';
    var breadcrumbs = [{"title":"DATASETS_ROOT", "urn":"page/1"}];
    updateActiveTab();
    $.get(url, function(data) {
      if (data && data.status == "ok"){
        datasetController.set('model', data);
        datasetController.set('breadcrumbs', breadcrumbs);
        datasetController.set('urn', null);
        datasetController.set('detailview', false);
      }
    });
    var watcherEndpoint = "/api/v1/urn/watch?urn=DATASETS_ROOT";
    $.get(watcherEndpoint, function(data){
        if(data.id && data.id !== 0) {
            datasetController.set('urnWatched', true)
            datasetController.set('urnWatchedId', data.id)
        } else {
            datasetController.set('urnWatched', false)
            datasetController.set('urnWatchedId', 0)
        }
    });
  },
  actions: {
    getDatasets: function(){
      var listUrl = 'api/v1/list/datasets';
      $.get(listUrl, function(data) {
        if (data && data.status == "ok"){
          renderDatasetListView(data.nodes);
        }
      });
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Datasets';
      updateActiveTab();
      $.get(url, function(data) {
        if (data && data.status == "ok"){
          datasetController.set('model', data);
          datasetController.set('urn', null);
          datasetController.set('detailview', false);
        }
      });
    }
  }
});

App.SubpageRoute = Ember.Route.extend({
  setupController: function(controller, param) {
    if(!datasetController)
      return;

    var listUrl = 'api/v1/list/datasets?urn=' + param.urn;
    var addSlash = false;
    if (param.urn && (param.urn[param.urn.length-1] != '/'))
    {
      addSlash = true;
    }
    if (addSlash)
    {
      listUrl += '/';
    }
    $.get(listUrl, function(data) {
      if (data && data.status == "ok"){
        renderDatasetListView(data.nodes);
      }
    });
    var url = 'api/v1/datasets?size=10&page=' + param.page + '&urn=' + param.urn;
    if (addSlash)
    {
      url += '/';
    }
    currentTab = 'Datasets';
    updateActiveTab();
    var breadcrumbs = [{"title":"DATASETS_ROOT", "urn":"page/1"}];
    var urn = param.urn.replace("://", "");
    var b = urn.split('/');
    for(var i = 0; i < b.length; i++) {
        if( i === 0) {
            breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + b[i] + ':///'
            })
        } else {
            var urn = b.slice(0, (i+1)).join('/');
            breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + param.urn.split('/').splice(0, i+3).join('/')
            })
        }
    }
    var watcherEndpoint = "/api/v1/urn/watch?urn=" + param.urn;
    if (addSlash)
    {
      watcherEndpoint += '/';
    }
    $.get(watcherEndpoint, function(data){
        if(data.id && data.id !== 0) {
            datasetController.set('urnWatched', true)
            datasetController.set('urnWatchedId', data.id)
        } else {
            datasetController.set('urnWatched', false)
            datasetController.set('urnWatchedId', 0)
        }
    });

    if (datasetController.currentName && param.urn)
    {
      findAndActiveDatasetNode(datasetController.currentName, param.urn);
    }

    $.get(url, function(data) {
      if (data && data.status == "ok"){
        datasetController.set('model', data);
        datasetController.set('urn', param.urn);
        datasetController.set('breadcrumbs', breadcrumbs);
        datasetController.set('detailview', false);
      }
    });
  },
  actions: {
    getDatasets: function(){
      var url = 'api/v1/datasets?size=10&page=' +
          datasetController.get('model.data.page') +
          '&urn=' +
          datasetController.get('urn');
      currentTab = 'Datasets';
      updateActiveTab();
      $.get(url, function(data) {
        if (data && data.status == "ok"){
          datasetController.set('model', data);
          datasetController.set('urn', datasetController.get('urn'));
          datasetController.set('detailview', false);
        }
      });
    }
  }
});
