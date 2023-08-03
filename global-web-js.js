var getHttpRequest = function () {
  var httpRequest = false;

  if (window.XMLHttpRequest) { // Mozilla, Safari,...
    httpRequest = new XMLHttpRequest();
    if (httpRequest.overrideMimeType) {
      httpRequest.overrideMimeType('text/xml');
    }
  }
  else if (window.ActiveXObject) { // IE
    try {
      httpRequest = new ActiveXObject("Msxml2.XMLHTTP");
    }
    catch (e) {
      try {
        httpRequest = new ActiveXObject("Microsoft.XMLHTTP");
      }
      catch (e) {}
    }
  }

  if (!httpRequest) {
    alert('Abandon :( Impossible de créer une instance XMLHTTP');
    return false;
  }

  return httpRequest
}


var ajaxKB = (conf)=> new Promise((resolve, reject)=>{
    var xhr = getHttpRequest();
    let loader = conf.loader;
    if (loader instanceof Element){
        loader.style.display = "block";
    }
	xhr.onreadystatechange = function() {

		if (this.readyState == 4) {
		    if (loader instanceof Element){
                loader.style.display = "none";
            }
            if (this.status == 200) resolve(this.responseText);
            else reject(this);
		}
	};
    var data_form = new FormData();
    let method = conf.method || "POST";
    let data = conf.data || {};
    if (method.toUpperCase() == "POST")
    for(d of Object.keys(data)){
        data_form.append(d, data[d]);
    }

    xhr.open(method , conf.url, true);
    //xhr.setRequestHeader('X-Requested-With', 'xmlhttprequest')
    if (method.toUpperCase() == "POST"){xhr.send(data_form);}
    else xhr.send();

});


async function postData(url = '', data = {}, method="POST", loader=null, get_json=true) {
  // Default options are marked with *
  let arg = {
    method: method.toUpperCase(), // *GET, POST, PUT, DELETE, etc.
    cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
    headers: {
     'Content-Type': 'application/json'
    //  'Content-Type': 'application/x-www-form-urlencoded'
    },
  }
  if (Object.keys(data || {}).length && method.toLowerCase() !="get")
        arg["body"] = JSON.stringify(data);
  if (loader instanceof Element){
    loader.style.display = "block";
  }
  let response = await fetch(url, arg);
  if (loader instanceof Element){
    loader.style.display = "none";
  }
  try{
      if (response.ok){
        if (get_json)
            response = response.json();
        return response;
      }else{
        console.log("error")
        console.log(response);
      }
  }catch(e){ console.error(e)}

  return response;

}


function setCookie(cname, cvalue, exdays) {
  const d = new Date();
  d.setTime(d.getTime() + ((exdays || 30) * 24 * 60 * 60 * 1000));
  let expires = "expires="+d.toUTCString();
  document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}


function getCookie(cname) {
  let name = cname + "=";
  let ca = document.cookie.split(';');
  for(let i = 0; i < ca.length; i++) {
    let c = ca[i];
    while (c.charAt(0) == ' ') {
      c = c.substring(1);
    }
    if (c.indexOf(name) == 0) {
      return c.substring(name.length, c.length);
    }
  }
  return "";
}


function get_domain(url, proto=true){
    // protocole://hostname/pathname
    let domain = (new URL(url));
    return (proto? domain.protocol+"//" : "") + domain.hostname;
}


function get_query_string(url){
    let inter = (new URL(url)).searchParams;
    return [...inter].map((x)=>{return {"k": x[0], "v": x[1]}});
}

function set_query_string_in_url(baseUrl, params){
  const Url = new URL(baseUrl);
  const urlParams = new URLSearchParams(Url.search);
  for (const key in params) {
    if (params[key] !== undefined) {
      urlParams.set(key, params[key]);
    }
  }
  Url.search = urlParams.toString();
  return Url.toString();
};

function readFile(_path, _cb){

    fetch(_path, {mode:'same-origin'})   // <-- important

    .then(function(_res) {
        return _res.blob();
    })

    .then(function(_blob) {
        var reader = new FileReader();

        reader.addEventListener("loadend", function() {
            _cb(this.result);
        });

        reader.readAsText(_blob);
    });
};

function create_css(selector, rules) {

    if (rules instanceof Object){
        str_rules = [...Object.keys(rules)].map(
                            k=> k+":"+rules[k]).join(";\n\t")
        rules = "{\n\t"+str_rules+"\n}";
    }
    var style= document.querySelector("#cssAddedByJS");
    if (!style) {
        style=document.createElement('style');
        style.type="text/css";
        style.id="cssAddedByJS";
        document.querySelector("head").appendChild(style);
    }
    style.sheet.insertRule(`${selector}${rules}`, 0);
}


function myCustomDate(value){
   this.compare=function (date) {
        date=this.getOnlyYmd(date);
        return this.date.getTime()-date.getTime();
       // body...
   }
   this.getOnlyYmd=function (date) {
        if (date.date) {
            date=date.date;
        }
       date=new Date(date.getFullYear()+"-"+(date.getMonth()+1)+"-"+date.getDate());
       return date;
   }
    this.add=function (value){
        if (value.getDate) {
            //Object Date
        }
        if( value.trim){
            let c=new Date(value);
            this.date=new Date(this.date.getTime()+ c.getTime()   );
        }
        return this;

    }
    this.addDay=function (value){
        this.date.setDate( this.date.getDate()+value );
        return this;

    }
    this.isoShow=function () {
        if (!this.date) {return "....-..-.."}
        let m=((this.date.getMonth()+1)>9)?(this.date.getMonth()+1):"0"+(this.date.getMonth()+1)
        return this.date.getFullYear()+"-"+m+"-"+this.date.getDate();
    }
    this.toShow= function (){
        if (!this.date) {return null;}
        let m=((this.date.getMonth()+1)>9)?(this.date.getMonth()+1):"0"+(this.date.getMonth()+1);
        let h="";
        if (this.date.getHours() || this.date.getMinutes() || this.date.getSeconds()){
            let d =[this.date.getHours(),
                    this.date.getMinutes(),
                    this.date.getSeconds()].map(nb=>nb.toLocaleString('en-US', {
                minimumIntegerDigits: 2,
                useGrouping: false
              })
            )

            h = " "+d[0]+":"+d[1] + ":"+d[2];
        }

        return this.date.getDate().toLocaleString('en-US', {
                minimumIntegerDigits: 2,
                useGrouping: false
              })+"/"+m+"/"+this.date.getFullYear()+h
    }

    this.toString=function(){
        return this.toShow();
    }
    this.parse = function(date_value){
        MONTH = {
            1: ["janvier", "january", "janv", "jan", "ja"],
            2: ["février", "fevrier", "february", "fév", "fev", "feb", "fe"],
            3: ["mars", "march", "mar"],
            4: ["avril", "april", "avr", "apr", "ap", "av"],
            5: ["mai", "may"],
            6: ["juin", "june", "jun"],
            7: ["juillet", "july", "jul"],
            8: ["août", "aout", "august", "aug", "ao"],
            9: ["septembre", "september", "sept", "sep"],
            10: ["octobre", "october", "oct"],
            11: ["novembre", "november", "nov", "no"],
            12: ["décembre", "decembre", "december", "dec", "de"]
        }
        let now  = new Date();
        if (date_value==null) return null;
        date_value = date_value.toString().trim().toLowerCase();
        let test = new Date(date_value);
        if (test.getDate()) return test;

        let reg = date_value.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?:[A-Z](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?)?$/i);
        if (reg) return new Date(date_value)

        if (date_value.includes("now") || date_value.match(/[à|a]\s+l\s*'\s*instant/))
            return now;
        reg_1 = /\s(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s/
        reg_0 = /\s(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s/
        let dyear = now.getUTCFullYear(), dmonth=now.getUTCMonth()+1,
            dday=now.getUTCDate(), dhour=0, dminute=0, dsecond=0, dmicro = 0;
        let got = true;

        let year = dyear, month=dmonth, day=dday;
        if (date_value.includes("aujourd") || date_value.includes("today")){
            got = true;
        }else if ((" "+date_value+" ").match(reg_1)){
            let res = (" "+date_value+" ").match(reg_1);
            year = res[3]; month = res[2]; day=res[1];
        }else if ((" "+date_value+" ").match(reg_0)){
            let res = (" "+date_value+" ").match(reg_0);
            year = res[1]; month = res[2]; day=res[3];
        }else{
            let month_ref = {};
            let v = "";
            for (key of Object.keys(MONTH)){
                for (part of MONTH[key]){
                    v += part + "|";
                    month_ref[part] = key;
                }
            }
            v = v.slice(0,v.length-1);

            reg = new RegExp("\\s(?:(\\d{1,2})\\s)?(" + v + ")\\s(\\d{4})\\s", "i");
            if ((" "+date_value+" ").match(reg)){
                let res = (" "+date_value+" ").match(reg);
                day = res[1]; month=res[2]; year =res[3];
                month = month_ref[month];

            }else got=false;
        }
        if (got){

            let reg_hour = /\s(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?\s/;
            let hour=0, minute=0, second=0, micro = 0;

            if ((" "+date_value+" ").match(reg_hour)){
                let res = (" "+date_value+" ").match(reg_hour);
                hour=res[1]; minute=res[2]; second=res[3]; micro= res[4];
            }
            let int = parseInt;
            date_value = new Date(int(year), int(month-1), int(day),
                                int(hour || dhour),
                                int(minute || dminute),
                                int(second || dsecond)
                                );
            return date_value;
        }
    }

    // init
    if (!value) { this.date=null}else
    if (value.getTime) { this.date=value;}
    else{
        this.date=this.parse(value);
        if (!this.date) {
            try{
                this.date=new Date(value);
            }catch (e){
                this.date=null;
            }
        }
    }
}
function CopyToClipboard(elm, show=true) {

    let got = false;
    try{
        navigator.clipboard.writeText(elm["innerText"]);
    }catch(e){
        console.error(e)
    }
    try{
        if (document.selection) {
            var range = document.body.createTextRange();
            range.moveToElementText(elm);
            range.select().createTextRange();
            document.execCommand("copy");
            got = true;
        } else if (window.getSelection) {
            var range = document.createRange();
            range.selectNode(elm);
            window.getSelection().addRange(range);
            document.execCommand("copy");
            //alert("Text has been copied, now paste in the text-area")
            got = true;

        }

    }catch(e){
        console.error(e)
    }
    if (got && show){
        let span = elm.querySelector(".copied-message") || elm.parentNode.querySelector(".copied-message");
        span.style.display = "block";

        setTimeout(()=>{span.style.display = "none"}, 1000)

    }
}



