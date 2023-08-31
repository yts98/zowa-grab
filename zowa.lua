local urlcode = require("urlcode")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()
JSObj = (loadfile "JSObj.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore:gsub("^https://", "http://")] = true
  downloaded[ignore:gsub("^http://", "https://")] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    target[item] = true
    return true
  end
  return false
end

parse_args = function(url)
  local parsed_url = urlparse.parse(url)
  local args = {}
  urlcode.parsequery(parsed_url["query"], args)
  return args
end

get_NUXT = function(html)
  local obj, args = string.match(html, "<script>window%.__NUXT__=%(function%([^%(%)]*%){return ({.+})%((.*)%)%);</script>")
  if obj and args then
    return JSObj:decode(obj), args
  else
    return nil, nil
  end
end

find_item = function(url)
  local value = string.match(url, "^https?://zowa%.app/rtist/([0-9]+)$")
  local type_ = "rtist"
  local other = nil
  if not value then
    value = string.match(url, "^https?://zowa%.app/play/([0-9]+)$")
    type_ = "play"
  end
  if not value then
    local play, list = string.match(url, "^https?://zowa%.app/play/([0-9]+)%?list=([0-9]+)$")
    if play and list then
      value = play .. ":" .. list
    end
    type_ = "play-list"
  end
  if not value then
    value = string.match(url, "^https?://zowa%.app/feature/([0-9]+)$")
    type_ = "feature"
  end
  if not value then
    value = string.match(url, "^https?://zowa%.app/search/result%?tag=([0-9]+)$")
    type_ = "tag"
  end
  if not value then
    value = string.match(url, "^https?://zowa%.app/audios/([0-9]+)$")
    type_ = "audio"
  end
  if not value then
    value = string.match(url, "^https?://zowa%.app/video/([0-9]+)$")
    type_ = "video"
  end
  if not value then
    value = string.match(url, "^https?://zowa%.app/zch/threads/([0-9]+)$")
    type_ = "thread"
  end
  if value then
    return {
      ["value"]=value,
      ["type"]=type_,
      ["other"]=other
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    local item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      initial_allowed = false
      tries = 0
      retry_url = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if string.match(url, "/<")
    or string.match(url, "/index%.rdf$")
    or string.match(url, "/'%+urls")
    or string.match(url, "/'%+[A-Za-z%$%[%]_]+%+'$")
    or string.match(url, "/'%+[A-Za-z%$%[%]_]+%+'/s$")
    or string.match(url, "/'%+[A-Za-z%$%[%]_]+%.[A-Za-z%$%[%]_]+%+'$")
    or string.match(url, "/'%+[A-Za-z%$%[%]_]+%.[A-Za-z%$%[%]_]+%.[A-Za-z%$%[%]_]+%+'$")
    or string.match(url, "/%${[A-Za-z%$%[%]_]+}$")
    or string.match(url, "/[A-Za-z_]+%${[A-Za-z%$%[%]_]+}$")
    or string.match(url, "/%${[A-Za-z%$%[%]_]+}%${[A-Za-z%$%[%]_]+}$")
    or not string.match(url, "^https?://") then
    return false
  end

  for pattern, type_ in pairs({
    ["^https?://zowa%.app/rtist/([0-9]+)$"]="rtist",
    ["^https?://zowa%.app/play/([0-9]+)$"]="play",
    ["^https?://zowa%.app/play/([0-9]+)%?list=([0-9]+)$"]="play-list",
    ["^https?://zowa%.app/feature/([0-9]+)$"]="feature",
    ["^https?://zowa%.app/search/result%?tag=([0-9]+)$"]="tag",
    ["^https?://zowa%.app/audios/([0-9]+)$"]="audio",
    ["^https?://zowa%.app/videos/([0-9]+)$"]="video",
    ["^https?://zowa%.app/zch/threads/([0-9]+)$"]="thread",
    ["^(https?://cdn%-s3%.zowa%.app/.+%.m3u8)$"]="m3u8",
  }) do
    local match = nil
    local other1 = nil
    if type_ == "play-list" then
      match, other1 = string.match(url, pattern)
      match = match .. ":" .. other1
    else
      match = string.match(url, pattern)
    end
    if match then
      if type_ == "m3u8" then
        discover_item(discovered_outlinks, match)
        return false
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        return false
      else
        return true
      end
    end
  end

  if string.match(url, "^https?://api%.zowa%.app/api/v2/") then
    return true
  end

  if string.match(url, "^https://s3%-ap%-northeast%-1%.amazonaws%.com/zowa%-transcoder%-input/") then
    discover_item(discovered_outlinks, url)
    return false
  end

  if not false then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local NUXT_obj = nil
  local NUXT_args = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function fix_case(newurl)
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl, referer)
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      if string.match(url_, "^https?://api%.zowa%.app/") then
        table.insert(urls, {
          url=url_,
          headers={
            ["Access-From"] = "pwa",
            ["Origin"] = "https://zowa.app",
            ["Referer"] = "https://zowa.app/"
          }
        })
      elseif referer then
        table.insert(urls, {
          url=url_,
          headers={
            ["Referer"] = referer
          }
        })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function analyze_play(play)
    if type(play["id"]) == "number" then
      check("https://zowa.app/play/" .. play["id"])
    end
    if type(play["user_id"]) == "number" then
      check("https://zowa.app/rtist/" .. play["user_id"])
    end
    if type(play["video_url"]) == "string" then
      check(play["video_url"])
    end
    if type(play["tags"]) == "table" then
      for _, tag in pairs(play["tags"]) do
        if type(tag["id"]) == "number" then
          check("https://zowa.app/search/result?tag=" .. tag["id"])
        end
      end
    end
  end

  if string.match(url, "^https?://api%.zowa%.app/") and status_code ~= 404 then
    html = read_file(file)
    json = JSON:decode(html)
  end

  if item_type == "rtist" then
    if string.match(url, "^https?://zowa%.app/rtist/[0-9]+$") then
      html = read_file(file)
      NUXT_obj, NUXT_args = get_NUXT(html)
      if NUXT_obj and NUXT_obj["data"][0]["rtist"] and NUXT_obj["data"][0]["rtist"]["id"] then
        if tostring(NUXT_obj["data"][0]["rtist"]["id"]) ~= item_value then
          abort_item()
        end
        check("https://api.zowa.app/api/v2/users/" .. item_value .. "/total_like")
        check("https://api.zowa.app/api/v2/users/pwa/" .. item_value .. "/all_videos_count")
        check("https://api.zowa.app/api/v2/videos/pwa/users/" .. item_value .. "?sort=new")
        check("https://api.zowa.app/api/v2/videos/pwa/users/" .. item_value .. "?sort=views_desc")
        check("https://api.zowa.app/api/v2/users/" .. item_value .. "/likes")
      else
        print(item_name .. " may not exist")
      end
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/users/[0-9]+/likes$") then
      for _, liker in pairs(json) do
        if type(liker["id"]) == "number" then
          check("https://zowa.app/rtist/" .. liker["id"])
        end
      end
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/videos/pwa/users/[0-9]+%?[^?]*$") then
      for _, play in pairs(json) do
        analyze_play(play)
      end
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/users/[0-9]+/total_like$") then
      if json["to_user"] and type(json["to_user"]["id"]) == "number" then
        check("https://zowa.app/rtist/" .. json["to_user"]["id"])
      end
    end
  end

  if item_type == "play" or item_type == "play-list" then
    if string.match(url, "^https?://zowa%.app/play/[0-9]+$") or string.match(url, "^https?://zowa%.app/play/[0-9]+%?list=[0-9]+$") then
      html = read_file(file)
      NUXT_obj, NUXT_args = get_NUXT(html)
      if NUXT_obj and NUXT_obj["data"][0]["videoDetails"] and NUXT_obj["data"][0]["videoDetails"]["id"] then
        analyze_play(NUXT_obj["data"][0]["videoDetails"])
      else
        print(item_name .. " may not exist")
      end
    end
  end

  if item_type == "feature" then
    if string.match(url, "^https?://zowa%.app/feature/[0-9]+$") then
      check("https://api.zowa.app/api/v2/videos/feature/" .. item_value)
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/videos/feature/[0-9]+$") then
      if status_code == 404 then
        print(item_name .. " may not exist")
      else
        if type(json["list_id"]) == "number" then
          check("https://api.zowa.app/api/v2/lists/show%?list_id=" .. json["list_id"])
          for _, play in pairs(json["videos"]) do
            check("https://zowa.app/play/" .. play["id"] .. "?list=" .. json["list_id"])
            analyze_play(play)
          end
        else
          print("Cannot find list_id from " .. url)
          abort_item()
        end
      end
    -- referenced by play-list
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/lists/show%?list_id=[0-9]+$") then
      for _, play in pairs(json["videos"]) do
        check("https://zowa.app/play/" .. play["id"] .. "?list=" .. json["list_id"])
        analyze_play(play)
      end
    end
  end

  if item_type == "tag" then
    if string.match(url, "^https?://zowa%.app/search/result%?tag=[0-9]+$") then
      for duration in {"&duration_start=30", "&duration_end=30", "&duration_end=10", ""} do
        for voice_kinds in {"1,2,0", "2,0", "1,0", "1,2", "0", "2", "1", ""} do
          check("https://api.zowa.app/api/v2/videos/pwa?sort=new&tags=" .. item_value .. duration .. (string.len(voice_kinds) >= 1 and ("&voice_kinds=" .. voice_kinds) or ""))
        end
      end
    elseif string.match(url, "^https?://api%.zowa%.app/api/v2/videos/pwa%?[^?]*") then
      for _, play in pairs(json) do
        analyze_play(play)
      end
    end
  end

  if item_type == "audio" then
    print(item_type .. " is not implemented yet")
    abort_item()
  end

  if item_type == "video" then
    print(item_type .. " is not implemented yet")
    abort_item()
  end

  if item_type == "thread" then
    print(item_type .. " is not implemented yet")
    abort_item()
  end

  if allowed(url)
    and status_code < 300 then
    local is_html = false
    if html == nil then
      html = read_file(file)
    end
    if string.match(url, "^https?://api%.zowa%.app/") then
      html = flatten_json(JSON:decode(html))
      is_html = true
    end
    if is_html then
      for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
        checknewurl(newurl)
      end
      for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
        checknewurl(newurl)
      end
      for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
        checknewshorturl(newurl)
      end
      for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
        checknewshorturl(newurl)
      end
      for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
        checknewurl(newurl)
      end
      html = string.gsub(html, "&gt;", ">")
      html = string.gsub(html, "&lt;", "<")
      for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
        checknewurl(newurl)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  local status_code = http_stat["statcode"]
  local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  if status_code ~= 200
    and status_code ~= 301
    and status_code ~= 302
    and status_code ~= 404 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response.")
    io.stdout:flush()
    tries = tries + 1
    if tries > 5 then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write(" Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()

  for key, data in pairs({
    ["zowa-0000000000000000"] = discovered_items,
    ["urls-0000000000000000"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


