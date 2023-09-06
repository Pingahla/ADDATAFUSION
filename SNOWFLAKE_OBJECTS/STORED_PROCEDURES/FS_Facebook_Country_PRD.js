CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_ADS_COUNTRY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
AS
$$

   var v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_country_array, v_load_dttm,
   v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
   v_video_30_sec_watched_action_array, v_video_p100_watched_action_array, v_video_p25_watched_action_array,
   v_video_p50_watched_action_array, v_video_p75_watched_action_array, v_website_click_through_array, v_click_qty,
   v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt, v_per_unique_click_cost_amt,
   v_per_unique_inline_link_click_cost_amt, v_per_click_average_cost_amt, v_per_thousand_impressions_average_cost_amt,
   v_per_point_cost_amt, v_click_through_pct, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty,
   v_inline_link_click_through_pct, v_inline_post_engagement_qty, v_objective_dc, v_reach_qty, v_spend_amt, v_unique_click_qty,
   v_unique_click_through_pct, v_unique_inline_link_click_qty, v_unique_inline_link_click_through_pct,
   v_unique_link_click_through_pct;
   var v_version_count;
   var count = 0;
   
   
  try {
    var cur_fs_country = snowflake.execute(
      {sqlText: `SELECT CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID, COUNTRY_ARRAY,
      LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY,
      VIDEO_30_SEC_WATCHED_ACTION_ARRAY, VIDEO_P100_WATCHED_ACTION_ARRAY, VIDEO_P25_WATCHED_ACTION_ARRAY,
      VIDEO_P50_WATCHED_ACTION_ARRAY, VIDEO_P75_WATCHED_ACTION_ARRAY, WEBSITE_CLICK_THROUGH_ARRAY, CLICK_QTY,
      PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT, PER_UNIQUE_CLICK_COST_AMT,
      PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT, PER_CLICK_AVERAGE_COST_AMT, PER_THOUSAND_IMPRESSIONS_AVERAGE_COST_AMT,
      PER_POINT_COST_AMT, CLICK_THROUGH_PCT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY,
      INLINE_LINK_CLICK_THROUGH_PCT, INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_DC, REACH_QTY, SPEND_AMT, UNIQUE_CLICK_QTY,
      UNIQUE_CLICK_THROUGH_PCT, UNIQUE_INLINE_LINK_CLICK_QTY, UNIQUE_INLINE_LINK_CLICK_THROUGH_PCT,
      UNIQUE_LINK_CLICK_THROUGH_PCT FROM ADDATAFUSION.FACEBOOK_ADS.VW_FS_ADS_COUNTRY`}
    );
   
   while (cur_fs_country.next()) {

      v_cd2_ad_id = cur_fs_country.getColumnValue(1);
      v_cd2_adset_id = cur_fs_country.getColumnValue(2);
      v_cd2_campaign_id = cur_fs_country.getColumnValue(3);
      v_country_array = cur_fs_country.getColumnValue(4);
      v_load_dttm = cur_fs_country.getColumnValue(5);
      v_start_dttm = cur_fs_country.getColumnValue(6);
      v_stop_dttm = cur_fs_country.getColumnValue(7);
      v_action_array = cur_fs_country.getColumnValue(8);
      v_outbound_click_array = cur_fs_country.getColumnValue(9);
      v_unique_action_array = cur_fs_country.getColumnValue(10);
      v_unique_outbound_click_array = cur_fs_country.getColumnValue(11);
      v_video_30_sec_watched_action_array = cur_fs_country.getColumnValue(12);
      v_video_p100_watched_action_array = cur_fs_country.getColumnValue(13);
      v_video_p25_watched_action_array = cur_fs_country.getColumnValue(14);
      v_video_p50_watched_action_array = cur_fs_country.getColumnValue(15);
      v_video_p75_watched_action_array = cur_fs_country.getColumnValue(16);
      v_website_click_through_array = cur_fs_country.getColumnValue(17);
      v_click_qty = cur_fs_country.getColumnValue(18);
      v_per_inline_link_click_cost_amt = cur_fs_country.getColumnValue(19);
      v_per_inline_post_engagement_cost_amt = cur_fs_country.getColumnValue(20);
      v_per_unique_click_cost_amt = cur_fs_country.getColumnValue(21);
      v_per_unique_inline_link_click_cost_amt = cur_fs_country.getColumnValue(22);
      v_per_click_average_cost_amt = cur_fs_country.getColumnValue(23);
      v_per_thousand_impressions_average_cost_amt = cur_fs_country.getColumnValue(24);
      v_per_point_cost_amt = cur_fs_country.getColumnValue(25);
      v_click_through_pct = cur_fs_country.getColumnValue(26);
      v_frequency_qty = cur_fs_country.getColumnValue(27);
      v_impressions_qty = cur_fs_country.getColumnValue(28);
      v_inline_link_click_qty = cur_fs_country.getColumnValue(29);
      v_inline_link_click_through_pct = cur_fs_country.getColumnValue(30);
      v_inline_post_engagement_qty = cur_fs_country.getColumnValue(31);
      v_objective_dc = cur_fs_country.getColumnValue(32);
      v_reach_qty = cur_fs_country.getColumnValue(33);
      v_spend_amt = cur_fs_country.getColumnValue(34);
      v_unique_click_qty = cur_fs_country.getColumnValue(35);
      v_unique_click_through_pct = cur_fs_country.getColumnValue(36);
      v_unique_inline_link_click_qty = cur_fs_country.getColumnValue(37);
      v_unique_inline_link_click_through_pct = cur_fs_country.getColumnValue(38);
      v_unique_link_click_through_pct = cur_fs_country.getColumnValue(39);

      
      
      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
          sqlText:  `SELECT COUNT(*) FROM ADDATAFUSION.DM_ADS.FS_ADS_COUNTRY WHERE 
          CD2_AD_ID=? AND CD2_ADSET_ID=? AND CD2_CAMPAIGN_ID=? AND COUNTRY_ARRAY=? AND START_DTTM=?`,

          binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_country_array, 
          v_start_dttm]
        });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }
      
      if (v_version_count == 0) { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
            sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_ADS_COUNTRY (CD2_AD_ID, CD2_ADSET_ID,
            CD2_CAMPAIGN_ID, COUNTRY_ARRAY, LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY,
            UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY, VIDEO_30_SEC_WATCHED_ACTION_ARRAY,
            VIDEO_P100_WATCHED_ACTION_ARRAY, VIDEO_P25_WATCHED_ACTION_ARRAY, VIDEO_P50_WATCHED_ACTION_ARRAY,
            VIDEO_P75_WATCHED_ACTION_ARRAY, WEBSITE_CLICK_THROUGH_RATE_ARRAY, CLICK_QTY, PER_INLINE_LINK_CLICK_COST_AMT,
            PER_INLINE_POST_ENGAGEMENT_COST_AMT, PER_UNIQUE_CLICK_COST_AMT, PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT,
            PER_CLICK_AVERAGE_COST_AMT, PER_THOUSAND_IMPRESSIONS_AVERAGE_COST_AMT, PER_POINT_COST_AMT, CLICK_THROUGH_PCT,
            FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CLICK_THROUGH_PCT, INLINE_POST_ENGAGEMENT_QTY,
            OBJECTIVE_DC, REACH_QTY, SPEND_AMT, UNIQUE_CLICK_QTY, UNIQUE_CLICK_THROUGH_PCT, UNIQUE_INLINE_LINK_CLICK_QTY,
            UNIQUE_INLINE_LINK_CLICK_THROUGH_PCT, UNIQUE_LINK_CLICK_THROUGH_PCT) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_country_array, v_load_dttm,
            v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
            v_video_30_sec_watched_action_array, v_video_p100_watched_action_array, v_video_p25_watched_action_array,
            v_video_p50_watched_action_array, v_video_p75_watched_action_array, v_website_click_through_array, v_click_qty,
            v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt, v_per_unique_click_cost_amt,
            v_per_unique_inline_link_click_cost_amt, v_per_click_average_cost_amt, v_per_thousand_impressions_average_cost_amt,
            v_per_point_cost_amt, v_click_through_pct, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty,
            v_inline_link_click_through_pct, v_inline_post_engagement_qty, v_objective_dc, v_reach_qty, v_spend_amt, v_unique_click_qty,
            v_unique_click_through_pct, v_unique_inline_link_click_qty, v_unique_inline_link_click_through_pct,
            v_unique_link_click_through_pct]
          });
          stmt.execute();
          count++;

      }
   }
   return 'ADS Country stored procedure completed succesfully. Records added: ' + count;
   } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
   