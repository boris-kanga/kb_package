var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%(host)s",
            port: parseInt(2000)
        },
        bypassList: []
        }
    };
var _browser = null;
try{
    if (!!browser.proxy) _browser=browser;
    else _browser = chrome;
}catch(e){
    _browser = chrome
}

_browser.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%(user)s",
            password: "%(pwd)s"
        }
    };
}

_browser.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            []
);
/*
function proxyRequest(request_data) {
    return {
        type: "http",
        host: "%(host)s",
        port: parseInt(2000)
    };
}
console.log(_browser);
console.log(_browser.proxy);
_browser.proxy.onRequest.addListener(proxyRequest, {urls: ["<all_urls>"]});
*/