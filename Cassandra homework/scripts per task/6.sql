CREATE TABLE IF NOT EXISTS killervideos.videos (
	video_id timeuuid PRIMARY KEY, 
	added_date timestamp, 
	description text, 
	title text, 
	user_id uuid 
);