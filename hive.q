set hive.cli.print.header=true;

-- Creating tables
create table titles(title string)
row format delimited
STORED AS textfile;

create table links(from_page int, to_page string)
row format delimited
fields terminated by ':'
STORED AS textfile;

create table hubs(page_id int, page_title string, hub_score float);
create table auths(page_id int, page_title string, auth_score float);
create table out_links(from_page int, to_page int);

-- Loading Tables
load data inpath '/data/links-simple-sorted.txt' overwrite into table links;
load data inpath '/data/titles-sorted.txt' overwrite into table titles;

insert overwrite table out_links
select from_page, to_page  from
(select from_page, SPLIT(LTRIM(to_page),' ') as array_links from links) as sub_query
LATERAL VIEW explode(array_links) sub_query as to_page;

-- Count the number of rows in table out-links, N.
select count(*) from out_links;

-- Update the values of hub score in table hubs, by initializing all page scores to 1.
-- The inner join is used as a filter for dead ends
insert overwrite table hubs
select unique_pages_id.from_page as page_id, numbered_titles.title as page_title, 1.0 as hub_score from 
(select distinct(from_page) from out_links) as unique_pages_id
inner join
(SELECT row_number() over (ORDER BY title) as page_id, title from titles) as numbered_titles
ON unique_pages_id.from_page = numbered_titles.page_id;

-- ======================
-- Compute the hub and authority scores for each page using equations 2 and 3. Then apply equations 4 and 5.
-- ======================

-- updating auth table
insert overwrite table auths
select auths_aux.page_id as page_id, page_title, auth_score from 
(select page_id, sum(hub_score) as auth_score from (
select out_links.to_page as page_id, hubs.hub_score from out_links
left join
hubs
on out_links.from_page = hubs.page_id
) as sub_query
group by page_id) as auths_aux
left join
hubs
on auths_aux.page_id = hubs.page_id;

-- updating hub table
insert overwrite table hubs
select hubs_aux.page_id as page_id, page_title, hubs_aux.hub_score from
(select page_id, sum(auth_score) as hub_score from (
select out_links.from_page as page_id, auths.auth_score from out_links
left join
auths
on out_links.to_page = auths.page_id
) as sub_query
group by page_id) as hubs_aux
left join
hubs
on hubs_aux.page_id = hubs.page_id;


-- Normalize tables
insert overwrite table auths
select page_id, page_title, auth_score / power(sub_query.denominator,0.5) as auth_score from(
select page_id, page_title, auth_score, sum(power(auth_score,2)) over () as denominator from auths
) as sub_query;

insert overwrite table hubs
select page_id, page_title, hub_score / power(sub_query.denominator,0.5) as hub_score from(
select page_id, page_title, hub_score, sum(power(hub_score,2)) over () as denominator from hubs
) as sub_query;

-- a) Display the top 20 pages with the highest auth score; output their title, their auth score and their hub score.
select auth_score, hub_score, hubs.page_title from (select * from auths
order by auth_score desc
limit 20) as LHS
left join
hubs
on LHS.page_id = hubs.page_id
order by auth_score desc;

-- b) Display the top 20 pages with the highest hub score; output their title, their auth score and their hub score
select auth_score, hub_score, LHS.page_title as page_title from (select * from hubs
order by hub_score desc
limit 20) as LHS
left join
auths
on LHS.page_id = auths.page_id
order by hub_score desc;