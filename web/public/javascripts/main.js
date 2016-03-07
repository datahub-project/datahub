(function ($) {
  $(document).ready(function() {
    var token = $("#csrfToken").val();
    function renderAdvSearchDatasetSources(parent, sources)
    {
        if ((!parent) || (!sources) || sources.length == 0)
            return;

        var content = '';
        for (var i = 0; i < sources.length; i++)
        {
            content += '<label class="checkbox"><input type="checkbox" name="sourceCheckbox" value="';
            content += sources[i] + '"/>' + sources[i] + '</label>';
        }
        parent.append(content);
    }

    var datasetSourcesUrl = 'api/v1/advsearch/sources';
    $.get(datasetSourcesUrl, function(data) {
        if (data && data.status == "ok")
        {
            var advSearchSourceObj = $("#advSearchSource");
            if (advSearchSourceObj)
            {
                if (data.sources)
                {
                    renderAdvSearchDatasetSources(advSearchSourceObj, data.sources);
                }
            }
        }
    });

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

    $(window).resize(function(){
      var height = ($(window).height() * 0.99) - 82;
      $('#mainSplitter').height(height)
    })

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

String.prototype.toProperCase = function(){
    return this.replace(/\w\S*/g, function(txt){
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    })
}
