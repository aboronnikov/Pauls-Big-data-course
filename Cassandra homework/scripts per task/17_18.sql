CREATE TABLE IF NOT EXISTS killervideos.videos_by_tag_year(
	tag text,
	added_year int,
	video_id timeuuid,
	added_date timestamp,
	description text,
	title text,
	user_id uuid,
	PRIMARY KEY(tag, added_year, video_id)
) WITH CLUSTERING ORDER BY(added_year DESC);