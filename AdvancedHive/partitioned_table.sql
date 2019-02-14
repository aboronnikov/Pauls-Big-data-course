CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:tableName} (                                                                                                                                                              
    BidID string,                                                                                                                                                                                                  
    Timestamp_ string,                                                                                                                                                                                             
    iPinYouID string,                                                                                                                                                                                              
    UserAgent string,                                                                                                                                                                                              
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
PARTITIONED BY (CityID int)                                                                                                                                                                                        
ROW FORMAT DELIMITED                                                                                                                                                                                               
FIELDS TERMINATED BY ${hiveconf:delimiter}                                                                                                                                                                                          
STORED AS TEXTFILE                                                                                                                                                                                                 
LOCATION ${hiveconf:pathToFile}; 

