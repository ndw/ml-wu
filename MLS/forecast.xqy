xquery version "1.0-ml";

declare namespace xdmp-http="xdmp:http";
declare namespace wu="https://github.com/ndw/ml-wu/ns/wu";
declare namespace wconfig="https://github.com/ndw/ml-wu/ns/config";

declare option xdmp:mapping "false";

declare variable $apikey := doc("/etc/wu-config.xml")/wconfig:config/wconfig:apikey/string();

declare function local:get-forecast(
  $query as xs:string
) as xs:string?
{
  let $uri := concat('http://api.wunderground.com/api/', $apikey, '/forecast7day/q/',$query,'.json')
  let $get := if (empty($apikey)) then () else xdmp:http-get($uri)
  return
    if ($get[1]/xdmp-http:code = 200)
    then
      if (xdmp:node-kind($get[2]/node()) = "binary")
      then
        xdmp:binary-decode($get[2]/node(), "utf-8")
      else
        string($get[2])
    else
      ()
};

declare function local:jsontoxml(
  $json as xs:string
) as element()
{
  (: Fucking json; there must be a better way to do this :)
  let $map := xdmp:from-json($json)
  let $txt := substring-after(xdmp:quote($map), "map:map(")
  let $txt := substring($txt, 1, string-length($txt)-1)
  return
    local:dumpmap("toplevel", xdmp:unquote($txt)/map:map)
};

declare function local:dumpmap(
  $key as xs:string,
  $map as element(map:map)
) as element()
{
  let $values
    := for $entry in $map/map:entry
       for $value in $entry/map:value
       return
         if ($value/map:map)
         then local:dumpmap($entry/@key, $value/map:map)
         else element { fn:QName("", $entry/@key) } { string($value) }
  return
    element { fn:QName("", $key) } { $values }
};

declare function local:forecast(
  $query as xs:string,
  $day as element(forecastday)
) as element(wu:forecast)
{
  <wu:forecast xmlns:wu="https://github.com/ndw/ml-wu/ns/wu">
    <wu:query>{$query}</wu:query>
    <wu:last-updated>{current-dateTime()}</wu:last-updated>
    <wu:icon>{ $day/icon/string() }</wu:icon>
    <wu:skyicon>{ $day/skyicon/string() }</wu:skyicon>
    <wu:date>
      { format-number(xs:integer($day/date/year),'0000') }
      { "-" }
      { format-number(xs:integer($day/date/month),'00') }
      { "-" }
      { format-number(xs:integer($day/date/day),'00') }
    </wu:date>
    <wu:high>
      <wu:celsius>{ $day/high/celsius/string() }</wu:celsius>
      <wu:fahrenheit>{ $day/high/fahrenheit/string() }</wu:fahrenheit>
    </wu:high>
    <wu:low>
      <wu:celsius>{ $day/low/celsius/string() }</wu:celsius>
      <wu:fahrenheit>{ $day/low/fahrenheit/string() }</wu:fahrenheit>
    </wu:low>
    <wu:conditions>{ $day/conditions/string() }</wu:conditions>
    <wu:pop>{ $day/pop/string() }</wu:pop>
  </wu:forecast>
};

declare function local:update(
  $query as xs:string
) as element(wu:forecast)*
{
  let $trace := xdmp:log(concat("Updating forecast for ", $query))
  let $jsoncast := local:get-forecast($query)
  let $forecast := if (empty($jsoncast)) then () else local:jsontoxml($jsoncast)
  for $day in $forecast/forecast/simpleforecast/forecastday
  let $cast := local:forecast($query, $day)
  return
    (xdmp:document-insert(concat("/forecast/", xdmp:hash64($query), "/", $cast/wu:date, ".xml"), $cast),
     $cast)
};

declare function local:current-forecast(
  $query as xs:string,
  $date as xs:string
) as element(wu:forecast)?
{
  let $fn := concat("/forecast/", xdmp:hash64($query), "/", $date, ".xml")
  return
    doc($fn)/*
};

declare function local:process(
  $query as xs:string,
  $date as xs:string
) as xs:string
{
  let $current  := local:current-forecast($query, $date)
  let $outdated := empty($current)
                   or (xs:dateTime($current/wu:last-updated) + xs:dayTimeDuration("PT6H") < current-dateTime())
  let $forecast
    := if ($outdated)
       then
         let $update := local:update($query)
         return $update[wu:date = $date]
      else
        $current

  let $map := map:map()
  let $_   := map:put($map, "icon", string($forecast/wu:icon))
  let $_   := map:put($map, "skyicon", string($forecast/wu:skyicon))
  let $_   := map:put($map, "highc", string($forecast/wu:high/wu:celsius))
  let $_   := map:put($map, "highf", string($forecast/wu:high/wu:fahrenheit))
  let $_   := map:put($map, "lowc", string($forecast/wu:low/wu:celsius))
  let $_   := map:put($map, "lowf", string($forecast/wu:low/wu:fahrenheit))
  let $_   := map:put($map, "conditions", string($forecast/wu:conditions))
  let $_   := map:put($map, "pop", string($forecast/wu:pop))

  return
    if (empty($forecast))
    then
      "{}"
    else
      concat('{',
             string-join(
               for $key in map:keys($map)
               return
                 concat('"', $key, '": "', map:get($map, $key), '"'),
               ","),
              '}')
};

let $query    := xdmp:url-decode(xdmp:get-request-field("query"))
let $date     := xdmp:get-request-field("date")
let $callback := xdmp:get-request-field("callback")
let $forecast
  := if ($date castable as xs:date)
     then
       local:process($query, $date)
     else
       "{}"
return
  (xdmp:set-response-content-type('application/json; charset=utf-8'),
   if (empty($callback))
   then
     $forecast
   else
     concat($callback, '(', $forecast, ')'))
