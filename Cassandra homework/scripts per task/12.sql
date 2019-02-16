CREATE TABLE IF NOT EXISTS killervideos.videos_by_title_year (
	video_id timeuuid,
	added_year int,
	added_date timestamp, 
	description text, 
	title text, 
	user_id uuid,
	PRIMARY KEY((title, added_year))
);