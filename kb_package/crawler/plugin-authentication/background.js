var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%(host)s",
            port: parseInt(%(port)s)
        },
        bypassList: []
        }
    };

(chrome||browser).proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%(user)s",
            password: "%(pwd)s"
        }
    };
}

(chrome||browser).webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);

function proxyRequest(request_data) {
    return {
        type: "http",
        host: "%(host)s",
        port: parseInt(%(port)s)
    };
}
(browser||chrome).proxy.onRequest.addListener(proxyRequest, {urls: ["<all_urls>"]});