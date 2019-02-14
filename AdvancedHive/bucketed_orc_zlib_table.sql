CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:tableName} (
    cityid int,
    device string,
    browser string,
    os string,
    BidID string,
    Timestamp_ string,
    iPinYouID string,
    IP string,
    RegionID int,
    AdExchange int,
    Domain string,
    URL string,
    AnonymousURL string,
    AdSlotID string,
    AdSlotWidth int,
    AdSlotHeight int,
    AdSlotVisibility string,
    AdSlotFormat string,
    AdSlotFloorPrice decimal,
    CreativeID string,
    BiddingPrice decimal,
    AdvertiserID string,
    UserProfileIDs array<string>
)
CLUSTERED BY (cityid, device, browser, os) INTO 20 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ${hiveconf:delimiter}
STORED AS ORC
LOCATION ${hiveconf:pathToFile}
TBLPROPERTIES("orc.compress"="zlib");