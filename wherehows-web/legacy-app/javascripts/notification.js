(function(window){
  'use strict'

  var parseForSearch = function(data) {
    var output = {}
    var search = data.split('?')[1]
    search = data.split['&'];
    for(var i = 0; i < search.length; i++) {
      var key = search[i].split('=')[0]
      var value = search[i].split('=')[1]
      output[key] = value;
    }

    return output
  }
  var Notify = window.Notify = {}

  var welcomeNotification = function(){
    var notification = new Notification( 'Desktop Notification Activated'
    , { body: "You will now recieve all notifications via the native notification system"
      , icon: '/public/assets/images/wh-logo2x.png'
      }
    )
  }

  var isDebugMode = function(){
    /*
    if(window.location.search().debug) {
      return true
    }
    if(window.location.search().debug === false) {
      return false
    }
    if(window.location.absUrl().indexOf("linkedin.com") === -1) {
      return true
    }
    */
    return false
  }


  var Notify = window.Notify = {}
  /**
   * @property
   * returns {Boolen} - Native Notification Supported
   */
  Notify.nativeSupport = true
  /**
   * @function
   * @public
   * @returns {Boolean} - Should native notifications be used
   */
  Notify.useNative = function(){
    return Storage.get('nativeNotification') || false
  }
  /**
   * @function
   * @public
   * @description - Attempts to get permission for native notification
   * support
   */
  Notify.getPermission = function(){
    if( !("Notification" in window) ) {
      console.error('Native notification are not supported')
      Notify.nativeSupport = false
      return false
    } else if ( Notification.permission === "granted") {
      Storage.set('nativeNotification', true)
      welcomeNotification()
    } else if (Notification.permission !== 'denied') {
      Notification.requestPermission(function(permission){
        if(permission === 'granted') {
          Storage.set('nativeNotification', true)
          welcomeNotification()
        }
      })
    }
  }

  /**
   * Creates a toast or Native Notification
   * @function
   * @public
   * @params msg {String} - The secondary text
   * @params title {String} - The title text
   * @params type {String} - The toast type (info, success, error, warning)
   */
  Notify.toast = function(msg, title, type){
    var isNative = Notify.useNative() && Notify.nativeSupport
    switch(isNative) {
      case true:
        var n = new Notification( type.toUpperCase() + ' - ' + title
        , { icon: '/public/assets/images/wh-logo2x.png'
          , body: msg
          }
        )
        break;
      case false:
        switch(type.toLowerCase()) {
          case 'error':
            toastr.error(msg, title)
            break;
          case 'info':
            toastr.info(msg, title)
            break;
          case 'success':
            toastr.success(msg, title)
            break;
          case 'warning':
            toastr.warning(msg, title)
            break;
        }
        break;
    }
  }

  /**
   * Creates a toast that is only visible in debug mode
   * @function
   * @public
   * @params msg {String} - The secondary text
   * @params title {String} - The title text
   * @params type {String} - The toast type (info, success, error, warning)
   */
  Notify.debugToast = function(msg, title, type){
    var isNative = Notify.useNative()
    if(isDebugMode()) {
      switch(isNative) {
        case true:
          var n = new Notification( type.toUpperCase() + ' - ' + title
          , { icon: '/public/assets/images/wh-logo2x.png'
            , body: msg
            }
          )
          break;
        case false:
          switch(type.toLowerCase()) {
            case 'error':
              toastr.error(msg, title)
              break;
            case 'info':
              toastr.info(msg, title)
              break;
            case 'success':
              toastr.success(msg, title)
              break;
            case 'warning':
              toastr.warning(msg, title)
              break;
          }
          break;
      }
    }
  }

})(window)
