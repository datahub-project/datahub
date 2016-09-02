(function ($) {
  $(document).ready(function() {
    var token = $("#csrfToken").val();

    $("#settingsbtn").click(function(){
      var url ='/api/v1/user/me'
      $
      .get
      ( url
      , function(data) {
          var settings = data.user.userSetting || {}
          for(var key in settings) {
            $("form[name='settingsForm'] [name='" + key + "']").val(settings[key])
          }
        }
      ).fail(function(){
        console.error("Could not pull data")
      })
      $("#settingsModal").modal('show')
    });

    $("#submitSettingsForm").on('click', function(){
      var settings = {}
      $("form[name='settingsForm']").serializeArray().map(function(item){
        var name = item.name.replace(/([A-Z])/g, function($1){ return "_" + $1.toLowerCase(); })
        settings[name] = item.value
      })
      var token = $("#csrfToken").val().replace('/', '')
      var url = "/api/v1/usersettings/me"
      settings.csrfToken = token
      $.ajax({
        url: url,
        method: 'POST',
        headers: {
          'Csrf-Token': token
        },
        dataType: 'json',
        data: settings
      }).done(function(data, txt, xhr){
        $("#settingsModal").modal('hide')
        location.reload()
      }).fail(function(xhr, txt, err){
        console.log('Error: Could not update settings.')
      })
    })

    var currentTab = 'Datasets';

    var width = $(window).width()*0.99;
    var height = ($(window).height() * 0.99) - 82;

    $('#mainSplitter').height(height);

    $("#mainSplitter").splitter({
        type: "v",
        minLeft: 100,
        sizeLeft: 250,
        minRight: 100,
        dock: "left",
        dockSpeed: 200,
        dockKey: 'Z',
        accessKey: 'I'
    });

    var splitterResize = false;
    var skipResize = false;

    $('#listviewbtn').click(function(){
        $('#listviewbtn').removeClass('btn-primary');
        $('#listviewbtn').removeClass('btn-default');
        $('#treeviewbtn').removeClass('btn-primary');
        $('#treeviewbtn').removeClass('btn-default');
        $('#listviewbtn').addClass('btn-primary');
        $('#treeviewbtn').addClass('btn-default');
        $('#tree2').hide();
        $('#tree3').hide();
        $('#datasetlist').show();
        $('#flowlist').show();
    });

    $('#treeviewbtn').click(function(){
        $('#listviewbtn').removeClass('btn-primary');
        $('#listviewbtn').removeClass('btn-default');
        $('#treeviewbtn').removeClass('btn-primary');
        $('#treeviewbtn').removeClass('btn-default');
        $('#listviewbtn').addClass('btn-default');
        $('#treeviewbtn').addClass('btn-primary');
        $('#datasetlist').hide();
        $('#flowlist').hide();
        $('#tree2').show();
        $('#tree3').show();
    });

    $(window).resize(function() {
      if (skipResize)
        return;
      if (splitterResize)
        clearTimeout(splitterResize);
      skipResize = true;
      splitterResize = setTimeout(function() {
        var height = ($(window).height() * 0.99) - 82;
        var width = $(window).width()*0.99;
        $("#mainSplitter").css("height", height, "width", width).trigger("resize");
        skipResize = false;
      }, 500);
    });

    var markedRendererOverride = new marked.Renderer()
    markedRendererOverride.link = function(href, title, text) {
          return "<a href='" + href + "' title='" + (title || text) + "' target='_blank'>" + text + "</a>";
        }

    marked.setOptions({
      gfm: true,
      tables: true,
      renderer: markedRendererOverride
    })
  });

})(jQuery)

var convertQueryStringToObject = function() {
    var queryString = {}
    var uri = window.location.hash || window.location.search
    uri.replace(
        new RegExp("([^?=&]+)(=([^&]*))?", "g"),
        function($0, $1, $2, $3) {
            queryString[$1] = $3
        }
    )
    return queryString;
}

function resetCategoryActiveFlag(category)
{
    $('#categoryDatasets').removeClass('active');
    $('#categoryComments').removeClass('active');
    $('#categoryMetrics').removeClass('active');
    $('#categoryFlows').removeClass('active');
    $('#categoryJobs').removeClass('active');
    if (category.toLowerCase() == 'datasets')
    {
        $('#categoryDatasets').addClass('active');
    }
    else if (category.toLowerCase() == 'comments')
    {
        $('#categoryComments').addClass('active');
    }
    else if (category.toLowerCase() == 'metrics')
    {
        $('#categoryMetrics').addClass('active');
    }
    else if (category.toLowerCase() == 'flows')
    {
        $('#categoryFlows').addClass('active');
    }
    else if (category.toLowerCase() == 'jobs')
    {
        $('#categoryJobs').addClass('active');
    }
    currentCategory = category;
}

function updateSearchCategories(category)
{
    if (category.toLowerCase() == 'all')
    {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-list');
    }
    else if (category.toLowerCase() == 'datasets')
    {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-database');
    }
    else if (category.toLowerCase() == 'comments')
    {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-comment');
    }
    else if (category.toLowerCase() == 'metrics')
    {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-plus-square-o');
    }
    else if (category.toLowerCase() == 'flows')
    {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-random');
    }
     else if (category.toLowerCase() == 'jobs')
     {
        $('#categoryIcon').removeClass('fa fa-list');
        $('#categoryIcon').removeClass('fa fa-database');
        $('#categoryIcon').removeClass('fa fa-comment');
        $('#categoryIcon').removeClass('fa fa-random');
        $('#categoryIcon').removeClass('fa fa-plus-square-o');
        $('#categoryIcon').removeClass('fa fa-file-o');
        $('#categoryIcon').addClass('fa fa-file-o');
     }
    resetCategoryActiveFlag(category);
}

String.prototype.toProperCase = function(){
    return this.replace(/\w\S*/g, function(txt){
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    })
}

function renderDatasetListView(nodes, name)
{
    var folderTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-folder"></i> $NODE_NAME</a>';
    var datasetTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-database"></i> $NODE_NAME</a>';
    var activeTemplate = '<a href="$URL" class="active list-group-item"><i class="fa fa-database"></i> $NODE_NAME</a>';
    var obj = $('#datasetlist');
    if (!obj)
        return;
    obj.empty();
    var activeObj;
    for(var i = 0; i < nodes.length; i++)
    {
        if (nodes[i].datasetId && nodes[i].datasetId > 0)
        {
            if (name && name == nodes[i].nodeName)
            {

                obj.append(activeTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
            }
            else
            {
                obj.append(datasetTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
            }
        }
        else
        {
            obj.append(folderTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
        }

    }
    if (activeObj)
    {
        var scrollToActiveNode = function() {
            $("#tabSplitter").scrollTo(activeObj, 800)
        }
    }
}

function renderFlowListView(nodes, flowId)
{
    var folderTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-folder"></i> $NODE_NAME</a>';
    var flowTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-random"></i> $NODE_NAME</a>';
    var activeTemplate = '<a href="$URL" class="active list-group-item"><i class="fa fa-random"></i> $NODE_NAME</a>';
    var obj = $('#flowlist');
    if (!obj)
        return;
    obj.empty();
    var activeObj;
    for(var i = 0; i < nodes.length; i++)
    {
        if (nodes[i].flowId && nodes[i].flowId > 0)
        {
            if (flowId == nodes[i].flowId)
            {
                activeObj = obj.append(activeTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
            }
            else
            {
                obj.append(flowTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
            }
        }
        else
        {
            obj.append(folderTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
        }

    }
    if (activeObj)
    {
        var scrollToActiveNode = function() {
            $("#tabSplitter").scrollTo(activeObj, 800)
        }
    }
}

function filterListView(category, filter)
{
    var obj = $('#flowlist');
    if (category == 'Datasets')
    {
        obj = $('#datasetlist');
    }

    if (!obj || !obj.children() || obj.children().length == 0)
    {
        return;
    }

    var items = obj.children();

    for(var i = 0; i < items.length; i++)
    {
        if (!filter)
        {
            $(items[i]).show();
        }
        else if (items[i].text && items[i].text.toLowerCase().includes(filter.toLowerCase()))
        {
            $(items[i]).show();
        }
        else
        {
            $(items[i]).hide();
        }
    }
}

function initializeColumnTreeGrid()
{
    $('#json-table').treegrid();
}

