/* This file is sourced during the initialization
 * of the crawler. Make sure everything is CREATE
 * IF NOT EXISTS, otherwise there will be errors
 * IF YOU CHANGE THIS FILE ALSO CHANGE test_values.py and parquet_schema.py
 * AND Schema-Documentation.md
 */

 



/* 
# custom tables 
*/
CREATE TABLE IF NOT EXISTS landing_page_categories (
  category_id INTEGER PRIMARY KEY NOT NULL,
  landing_page_id INTEGER,
  landing_page_url TEXT NOT NULL,
  category_code TEXT NOT NULL,
  category_name TEXT NOT NULL,
  parent_category TEXT NOT NULL,
  confident NOT NULL
);

CREATE TABLE IF NOT EXISTS landing_pages (
  landing_page_id INTEGER PRIMARY KEY NOT NULL,
  landing_page_url TEXT NOT NULL UNIQUE,
  categorized BOOLEAN DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS visit_advertisements (
    ad_id INTEGER PRIMARY KEY,
    visit_id INTEGER,
    browser_id INTEGER,
    ad_url TEXT,
    visit_url TEXT,
    clean_run BOOLEAN,
    -- to access the ad's corresponding "number" within the visit
    -- when we have multiple "possible urls" for the same ad, we can use this number to refer to the specific ad, also useful for the screenshot
    ad_number_in_visit INTEGER,
    sub_ad_number_in_chumbox INTEGER DEFAULT NULL,
    chumbox_platform TEXT DEFAULT NULL,
    landing_page_id INTEGER DEFAULT NULL,
    landing_page_url TEXT DEFAULT NULL,
    categorized BOOLEAN DEFAULT FALSE,
    oba_potential BOOLEAN DEFAULT FALSE,
    non_ad BOOLEAN DEFAULT NULL,
    unspecific_ad BOOLEAN DEFAULT NULL,
    FOREIGN KEY(visit_id) REFERENCES visits(visit_id),
    FOREIGN KEY(browser_id) REFERENCES crawl(browser_id),
    FOREIGN KEY(landing_page_id) REFERENCES ad_landing_page(landing_page_id)
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id INTEGER PRIMARY KEY,
    domain VARCHAR(100),
	url VARCHAR(200),
--     type 1: training, 2: control, 3: clean_run
  type INTEGER DEFAULT NULL,
	run_url VARCHAR(200),
	status INTEGER,
	lang VARCHAR(50),
	banners INTEGER DEFAULT 0,
	btn_status INTEGER,
    btn_set_status INTEGER,
    interact_time INTEGER,
	ttw INTEGER,
	__cmp BOOLEAN DEFAULT FALSE,
	__tcfapi BOOLEAN DEFAULT FALSE,
	__tcfapiLocator DEFAULT FALSE,
	cmp_id INTEGER,
	cmp_name VARCHAR(100),
	pv BOOLEAN DEFAULT FALSE,
    nc_cmp_name VARCHAR(100),
    dnsmpi VARCHAR(100),
    body_html TEXT
);

CREATE TABLE IF NOT EXISTS banners (
	banner_id INTEGER PRIMARY KEY,
    visit_id INTEGER,
    domain VARCHAR(100),
    lang VARCHAR(50),
	iFrame BOOLEAN DEFAULT FALSE,
    shadow_dom BOOLEAN DEFAULT FALSE,
	captured_area FLOAT,
	x INTEGER,
	y INTEGER,
	w INTEGER,
	h INTEGER,
	FOREIGN KEY(visit_id) REFERENCES visits(visit_id)
);

CREATE TABLE IF NOT EXISTS htmls (
	banner_id INTEGER PRIMARY KEY,
    visit_id INTEGER,
    domain VARCHAR(100),
    html TEXT,
    FOREIGN KEY(visit_id) REFERENCES visits(visit_id)
);
/* custom tables end*/



CREATE TABLE IF NOT EXISTS task (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    manager_params TEXT NOT NULL,
    openwpm_version TEXT NOT NULL,
    browser_version TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS crawl (
    browser_id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL,
    browser_params TEXT NOT NULL,
    start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(task_id) REFERENCES task(task_id));

/*
# site_visits
 */
CREATE TABLE IF NOT EXISTS site_visits (
    visit_id INTEGER PRIMARY KEY,
    browser_id INTEGER NOT NULL,
    site_url VARCHAR(500) NOT NULL,
    site_rank INTEGER,
    FOREIGN KEY(browser_id) REFERENCES crawl(browser_id));

/*
# crawl_history
 */
CREATE TABLE IF NOT EXISTS crawl_history (
    browser_id INTEGER,
    visit_id INTEGER,
    command TEXT,
    arguments TEXT,
    retry_number INTEGER,
    command_status TEXT,
    error TEXT,
    traceback TEXT,
    duration INTEGER,
    dtg DATETIME DEFAULT (CURRENT_TIMESTAMP),
    FOREIGN KEY(browser_id) REFERENCES crawl(browser_id));

/*
# http_requests
 */
CREATE TABLE IF NOT EXISTS http_requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  incognito INTEGER,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  extension_session_uuid TEXT,
  event_ordinal INTEGER,
  window_id INTEGER,
  tab_id INTEGER,
  frame_id INTEGER,
  url TEXT NOT NULL,
  top_level_url TEXT,
  parent_frame_id INTEGER,
  frame_ancestors TEXT,
  method TEXT NOT NULL,
  referrer TEXT NOT NULL,
  headers TEXT NOT NULL,
  request_id INTEGER NOT NULL,
  is_XHR INTEGER,
  is_third_party_channel INTEGER,
  is_third_party_to_top_window INTEGER,
  triggering_origin TEXT,
  loading_origin TEXT,
  loading_href TEXT,
  req_call_stack TEXT,
  resource_type TEXT NOT NULL,
  post_body TEXT,
  post_body_raw TEXT,
  time_stamp DATETIME NOT NULL
);

/*
# http_responses
 */
CREATE TABLE IF NOT EXISTS http_responses(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  incognito INTEGER,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  extension_session_uuid TEXT,
  event_ordinal INTEGER,
  window_id INTEGER,
  tab_id INTEGER,
  frame_id INTEGER,
  url TEXT NOT NULL,
  method TEXT NOT NULL,
  response_status INTEGER,
  response_status_text TEXT NOT NULL,
  is_cached INTEGER NOT NULL,
  headers TEXT NOT NULL,
  request_id INTEGER NOT NULL,
  location TEXT NOT NULL,
  time_stamp DATETIME NOT NULL,
  content_hash TEXT
);

/*
# http_redirects
 */
CREATE TABLE IF NOT EXISTS http_redirects(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  incognito INTEGER,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  old_request_url TEXT,
  old_request_id TEXT,
  new_request_url TEXT,
  new_request_id TEXT,
  extension_session_uuid TEXT,
  event_ordinal INTEGER,
  window_id INTEGER,
  tab_id INTEGER,
  frame_id INTEGER,
  response_status INTEGER NOT NULL,
  response_status_text TEXT NOT NULL,
  headers TEXT NOT NULL,
  time_stamp DATETIME NOT NULL
);

/*
# javascript
 */
CREATE TABLE IF NOT EXISTS javascript(
  id INTEGER PRIMARY KEY,
  incognito INTEGER,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  extension_session_uuid TEXT,
  event_ordinal INTEGER,
  page_scoped_event_ordinal INTEGER,
  window_id INTEGER,
  tab_id INTEGER,
  frame_id INTEGER,
  script_url TEXT,
  script_line TEXT,
  script_col TEXT,
  func_name TEXT,
  script_loc_eval TEXT,
  document_url TEXT,
  top_level_url TEXT,
  call_stack TEXT,
  symbol TEXT,
  operation TEXT,
  value TEXT,
  arguments TEXT,
  time_stamp DATETIME NOT NULL
);

/*
# javascript_cookies
 */
CREATE TABLE IF NOT EXISTS javascript_cookies(
    id INTEGER PRIMARY KEY ASC,
    browser_id INTEGER NOT NULL,
    visit_id INTEGER NOT NULL,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    record_type TEXT,
    change_cause TEXT,
    expiry DATETIME,
    is_http_only INTEGER,
    is_host_only INTEGER,
    is_session INTEGER,
    host TEXT,
    is_secure INTEGER,
    name TEXT,
    path TEXT,
    value TEXT,
    same_site TEXT,
    first_party_domain TEXT,
    store_id STRING,
    time_stamp DATETIME
);

/*
# Navigations
 */
CREATE TABLE IF NOT EXISTS navigations(
  id INTEGER,
  incognito INTEGER,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  extension_session_uuid TEXT,
  process_id INTEGER,
  window_id INTEGER,
  tab_id INTEGER,
  tab_opener_tab_id INTEGER,
  frame_id INTEGER,
  parent_frame_id INTEGER,
  window_width INTEGER,
  window_height INTEGER,
  window_type TEXT,
  tab_width INTEGER,
  tab_height INTEGER,
  tab_cookie_store_id TEXT,
  uuid TEXT,
  url TEXT,
  transition_qualifiers TEXT,
  transition_type TEXT,
  before_navigate_event_ordinal INTEGER,
  before_navigate_time_stamp DATETIME,
  committed_event_ordinal INTEGER,
  committed_time_stamp DATETIME
);

/*
# Callstacks
 */
CREATE TABLE IF NOT EXISTS callstacks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  call_stack TEXT
);

/*
 # Logging all interrupted visits
 */
CREATE TABLE IF NOT EXISTS incomplete_visits (
   visit_id INTEGER NOT NULL
);

/* 
# DNS Requests
 */
CREATE TABLE IF NOT EXISTS dns_responses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  browser_id INTEGER NOT NULL,
  visit_id INTEGER NOT NULL,
  hostname TEXT,
  addresses TEXT,
  used_address TEXT,
  canonical_name TEXT,
  is_TRR INTEGER, 
  time_stamp DATETIME NOT NULL
 );





