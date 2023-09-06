--#############################################################
-- Facebook Average Clicks and Costs per Age Range and Gender
--#############################################################

SELECT
 'FACEBOOK'                            AS SOURCE_DC
,FSAG.AGE_RANGE_DC                     AS AGE_RANGE_DC
,FSAG.GENDER_DC                        AS GENDER_DC
--,CAST(FSAG.START_DTTM AS DATE)         AS START_DT
--,CAST(FSAG.STOP_DTTM AS DATE)          AS STOP_DT
,AVG(FSAG.CLICK_QTY)                   AS AVERAGE_CLICK_QTY
,AVG(FSAG.PER_UNIQUE_CLICK_COST_AMT)   AS PER_CONVERSION_COST_AMT
FROM DM_ADS.FS_ADS_AGE_AND_GENDER      AS FSAG
WHERE FSAG.CLICK_QTY                 <> 0
   OR FSAG.PER_UNIQUE_CLICK_COST_AMT <> 0
GROUP BY
 FSAG.AGE_RANGE_DC
,FSAG.GENDER_DC
--,CAST(FSAG.START_DTTM AS DATE)
--,CAST(FSAG.STOP_DTTM AS DATE)
ORDER BY 1,2,3,4

--#############################################################
-- Facebook Average Clicks and Costs by Country
--#############################################################

SELECT
 'FACEBOOK'                            AS SOURCE_DC
,C.COUNTRY_ARRAY                       AS COUNTRY_ARRAY
--,CAST(C.START_DTTM AS DATE)          AS START_DT
--,CAST(C.STOP_DTTM AS DATE)           AS STOP_DT
,AVG(C.CLICK_QTY)                      AS AVERAGE_CLICK_QTY
,AVG(C.PER_UNIQUE_CLICK_COST_AMT)      AS PER_CONVERSION_COST_AMT
FROM DM_ADS.FS_ADS_COUNTRY             AS C
WHERE C.CLICK_QTY                 <> 0
   OR C.PER_UNIQUE_CLICK_COST_AMT <> 0
GROUP BY
 COUNTRY_ARRAY
--,CAST(C.START_DTTM AS DATE)
--,CAST(C.STOP_DTTM AS DATE)
ORDER BY 1,2,3,4

--#############################################################
-- Facebook Average Clicks and Costs by Platform and Device
--#############################################################

SELECT
 'FACEBOOK'                            AS SOURCE_DC
,A.PLATFORM_POSITION_DC                AS PLATFORM_POSITION_DC
,A.PUBLISHER_PLATFORM_DC               AS PUBLISHER_PLATFORM_DC
--,CAST(A.START_DTTM AS DATE)          AS START_DT
--,CAST(A.STOP_DTTM AS DATE)           AS STOP_DT
,AVG(A.CLICK_QTY)                      AS AVERAGE_CLICK_QTY
,AVG(A.PER_UNIQUE_CLICK_COST_AMT)      AS PER_CONVERSION_COST_AMT
FROM DM_ADS.FS_ADS_PLATFORM_AND_DEVICE AS A
WHERE A.CLICK_QTY                 <> 0
   OR A.PER_UNIQUE_CLICK_COST_AMT <> 0
GROUP BY
 A.PLATFORM_POSITION_DC
,A.PUBLISHER_PLATFORM_DC
--,CAST(A.START_DTTM AS DATE)
--,CAST(A.STOP_DTTM AS DATE)
ORDER BY 1,2,3,4

--#############################################################
-- Facebook Average Clicks and Costs by Region
--#############################################################

SELECT
 'FACEBOOK'                            AS SOURCE_DC
,A.REGION_DC                           AS REGION_DC
--,CAST(A.START_DTTM AS DATE)          AS START_DT
--,CAST(A.STOP_DTTM AS DATE)           AS STOP_DT
,AVG(A.CLICK_QTY)                      AS AVERAGE_CLICK_QTY
,AVG(A.PER_UNIQUE_CLICK_COST_AMT)      AS PER_CONVERSION_COST_AMT
FROM DM_ADS.FS_ADS_REGION              AS A
WHERE A.CLICK_QTY                 <> 0
   OR A.PER_UNIQUE_CLICK_COST_AMT <> 0
GROUP BY
 A.REGION_DC
--,CAST(A.START_DTTM AS DATE)
--,CAST(A.STOP_DTTM AS DATE)
ORDER BY 1,2,3,4

--#############################################################
-- Google Average Clicks by Ad Network Type and Campaign
--#############################################################

SELECT
 'GOOGLE'                                   AS SOURCE_DC
,A.AD_NETWORK_TYPE_DC                       AS AD_NETWORK_TYPE_DC
,A.CAMPAIGN_NM                              AS CAMPAIGN_NM
,A.CLICK_TYPE_DC                            AS CLICK_TYPE_DC
--,A.LOAD_DTTM                                AS LOAD_DTTM
,AVG(A.CLICKS_QTY)                          AS AVERAGE_CLICK_QTY
FROM DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_CLICK AS A
WHERE A.CLICKS_QTY                 <> 0
GROUP BY
 A.AD_NETWORK_TYPE_DC
,A.CAMPAIGN_NM
,A.CLICK_TYPE_DC
--,A.LOAD_DTTM
ORDER BY 1,2,3,4

--#############################################################
-- Linked-In Average Clicks and Cost by Pivot and Campaign
--#############################################################

SELECT
 'Linked-In'                                      AS SOURCE_DC
,A.CAMPAIGN_DC                                    AS CAMPAIGN_DC
,A.PIVOT_DC                                       AS PIVOT_DC
--,A.START_AT_DTTM                                AS START_AT_DTTM
--,A.END_AT_DTTM                                  AS END_AT_DTTM
,AVG(A.CLICKS_QTY)                                AS AVERAGE_CLICK_QTY
,AVG(A.COST_IN_USD_AMT)                           AS AVERAGE_COST_AMT
FROM DM_ADS.FS_LINKEDIN_ADS_ANALYTICS_BY_CAMPAIGN AS A
WHERE A.CLICKS_QTY                 <> 0
   OR A.COST_IN_USD_AMT            <> 0
GROUP BY
 A.CAMPAIGN_DC
,A.PIVOT_DC
--,A.START_AT_DTTM
--,A.END_AT_DTTM
ORDER BY 1,2,3,4

--#############################################################
-- TikTok Average Clicks and Cost by Various Dimensions
--#############################################################

SELECT
 'TIKTOK'                                            AS SOURCE_DC
,A.ADGROUP_NM                                        AS ADGROUP_NM
,A.AD_NM                                             AS AD_NM
,A.CAMPAIGN_NM                                       AS CAMPAIGN_NM
,A.GENDER_DC                                         AS GENDER_DC
,A.PROMOTION_TYPE_DC                                 AS PROMOTION_TYPE_DC
,A.TIKTOK_APP_NM                                     AS TIKTOK_APP_NM
--,A.STAT_DTTM                                         AS STAT_DTTM
,AVG(A.CLICK_QTY)                                    AS AVERAGE_CLICK_QTY
,AVG(A.COST_PER_CLICK_AMT)                           AS AVERAGE_COST_AMT
FROM DM_ADS.FS_TIKTOK_ADS_INSIGHTS_BY_AGE_AND_GENDER AS A
WHERE A.CLICK_QTY                 <> 0
   OR A.COST_PER_CLICK_AMT        <> 0
GROUP BY
 A.ADGROUP_NM
,A.AD_NM
,A.CAMPAIGN_NM
,A.GENDER_DC
,A.PROMOTION_TYPE_DC
,A.TIKTOK_APP_NM
--,A.STAT_DTTM
ORDER BY 1,2,3,4
