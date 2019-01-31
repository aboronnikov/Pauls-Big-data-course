COPY killervideos.videos FROM '/home/pavel_orekhov/labwork/exercise-5/videos.csv' WITH HEADER = true;

CREATE TYPE IF NOT EXISTS killervideos.video_encoding (
	encoding TEXT,
	height INT,
	width INT,
	bit_rates SET<TEXT>,
);