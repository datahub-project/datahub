function initializeColumnTreeGrid()
{
  $('.columntreegrid').treegrid();
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

function setOwnerNameAutocomplete(controller)
{
  if (!controller)
  {
    return;
  }

  $('.userEntity').blur(function(data){
    var userEntitiesMaps = controller.get("userEntitiesMaps");
    var value = this.value;
    if (userEntitiesMaps[value])
    {
      controller.set("showMsg", false);
      var owners = controller.get("owners");
      for(var i = 0; i < owners.length; i++)
      {
        if (owners[i].userName == value)
        {
          Ember.set(owners[i], "name", userEntitiesMaps[value]);
          if (userEntitiesMaps[value])
          {
            Ember.set(owners[i], "isGroup", false);
          }
          else
          {
            Ember.set(owners[i], "isGroup", true);
          }
        }
      }
    }
    else
    {
      controller.set("showMsg", true);
      controller.set("alertType", "alert-danger");
      controller.set("ownerMessage", "The user name '" + value + "' is invalid");
    }
  });

  $('.userEntity').autocomplete({
    select: function( event, ui )
    {
      var userEntitiesMaps = controller.get("userEntitiesMaps");
      var value = ui.item.value;
      if (value in userEntitiesMaps)
      {
        var owners = controller.get("owners");
        for(var i = 0; i < owners.length; i++)
        {
          if (owners[i].userName == value)
          {
            controller.set("showMsg", false);
            Ember.set(owners[i], "name", userEntitiesMaps[value]);
            if (userEntitiesMaps[value])
            {
              Ember.set(owners[i], "isGroup", false);
            }
            else
            {
              Ember.set(owners[i], "isGroup", true);
            }
          }
        }
      }
    },

    source: function(request, response) {
      var userEntitiesSource = controller.get("userEntitiesSource");
      var results = $.ui.autocomplete.filter(userEntitiesSource, request.term);
      response(results.slice(0, 20));
    }

  });
}

function convertPropertiesToArray(properties)
{
  var propertyArray = [];
  if (properties)
    {
      for (var key in properties)
        {
          if (key.toLowerCase() != 'elements')
          {
            if (typeof properties[key] !== 'object')
            {
              propertyArray.push({'key':key, 'value':formatValue(key, properties[key])});
            }
            else
            {
              propertyArray.push({'key':key, 'value':JsonHuman.format(properties[key])});
            }

          }
        }
    }
    return propertyArray;
}

var datasetController = null;
var detailController = null;
App.DatasetsRoute = Ember.Route.extend({
  setupController: function(controller) {
    datasetController = controller;
  },
  actions: {
    getDatasets: function(){
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Dataset';
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
  setupController: function(controller, params) {
    var _this = this;
    detailController = controller;
    currentTab = 'Dataset';
    updateActiveTab();
    var id = 0;
    var source = '';
    var urn = '';
    controller.set("hasProperty", false);
    if(params && params.id)
      {
        id = params.id;
        source = params.source;
        urn = params.urn;
        datasetController.set("detailview", true);
        controller.set('model', params);

      }
      else {
        if (params.dataset)
          {
            id = params.dataset.id;
            source = params.dataset.source;
            urn = params.dataset.urn;
            controller.set('model', params.dataset);
            datasetController.set("detailview", true);
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
          var breadcrumbs = [];
          var updatedUrn = urn.replace("://", "");
          var b = updatedUrn.split('/');
          for(var i = 0; i < b.length; i++) {
            if( i === 0) {
              breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + b[i]
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
                  setTimeout(initializeColumnTreeGrid, 500);
                }
                else
                  {
                    controller.set("hasSchemas", false);
                    controller.buildJsonView();
                  }
            }
            else
              {
                controller.set("hasSchemas", false);
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
            else if (source.toLowerCase() == 'teradata')
              {
                sampleUrl = 'api/v1/datasets/' + id + "/sample";
                $.get(sampleUrl, function(data) {
                  if (data && data.status == "ok")
                    {

                      if (data.sampleData && data.sampleData.sample && data.sampleData.sample.columnNames
                          && (data.sampleData.sample.columnNames.length > 0))
                        {
                          controller.set("hasSamples", true);
                          controller.set("samples", data.sampleData.sample.data);
                          controller.set("columns", data.sampleData.sample.columnNames);
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
              else if (urn && urn.substring(0,7) == 'hdfs://')
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

    var ownershipUrl = 'api/v1/datasets/' + id + "/owners";
    $.get(ownershipUrl, function(data) {
          if (data && data.status == "ok")
          {
            var owners = [];
            if (data.owners && (data.owners.length > 0))
            {
              owners = data.owners;
            }
            controller.set("owners", owners);
            setTimeout(function(){
              $( "#sortedownertable" ).sortable({
                start: function(event, ui) {
                  var start_pos = ui.item.index();
                  ui.item.data('start_pos', start_pos);
                },
                update: function( event, ui ) {
                  var startPos = ui.item.data('start_pos');
                  var endPos = ui.item.index();
                  var originalOwners = controller.get("owners");
                  if (!originalOwners || originalOwners.length == 0)
                  {
                    return;
                  }
                  var owners = originalOwners.slice();
                  var updatedOwner = owners[startPos];
                  owners.splice(startPos, 1);
                  owners.splice(endPos, 0, updatedOwner);
                  originalOwners.setObjects(owners);
                  setTimeout(function(){
                    setOwnerNameAutocomplete(controller)
                  }, 500);
                }
              });

              var allUserEntitiesUrl = 'api/v1/company/entities';
              $.get(allUserEntitiesUrl, function(data) {
                if (data && data.status == "ok")
                {
                  if (data.userEntities && (data.userEntities.length > 0))
                  {
                    var userEntitiesMaps = {};
                    for (i=0; i < data.userEntities.length; i++)
                    {
                      userEntitiesMaps[data.userEntities[i].label] = data.userEntities[i].displayName;
                    }
                    var userEntitiesSource = Object.keys(userEntitiesMaps);
                    controller.set("userEntitiesMaps", userEntitiesMaps);
                    controller.set("userEntitiesSource", userEntitiesSource);
                    setOwnerNameAutocomplete(controller);
                  }
                }
              });
            }, 500);
          }
    });
  },
  model: function(params) {
    return Ember.$.getJSON('api/v1/datasets/' + params.id);
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
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Dataset';
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
    var url = 'api/v1/datasets?size=10&page=' + param.page;
    currentTab = 'Dataset';
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
      var url = 'api/v1/datasets?size=10&page=' + datasetController.get('model.data.page');
      currentTab = 'Dataset';
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
    var url = 'api/v1/datasets?size=10&page=' + param.page + '&urn=' + param.urn;
    currentTab = 'Dataset';
    updateActiveTab();
    var breadcrumbs = [];
    var urn = param.urn.replace("://", "");
    var b = urn.split('/');
    for(var i = 0; i < b.length; i++) {
        if( i === 0) {
            breadcrumbs.push({
                title: b[i],
                urn: "name/" + b[i] + "/page/1?urn=" + b[i]
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
      currentTab = 'Dataset';
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
