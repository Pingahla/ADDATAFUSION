CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_ADS_PLATFORM_AND_DEVICE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_position_dc, v_platform_dc, v_device_dc,
  v_load_dttm, v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
  v_video_30_sec_watched_action_array, v_video_p25_watched_action_array, v_video_p50_watched_action_array,
  v_video_p75_watched_action_array, v_website_click_through_array, v_click_qty, v_per_inline_link_click_cost_amt,
  v_per_inline_post_engagement_cost_amt, v_per_unique_click_cost_amt, v_per_unique_inline_link_click_cost_amt, v_average_cpc_cost_amt,
  v_average_cpm_cost_amt, v_per_point_cost_amt, v_click_through_rate_amt, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty,
  v_inline_link_click_through_pct, v_inline_post_engagement_qty, v_objective_dc, v_reach_qty, v_send_qty, v_unique_click_qty,
  v_unique_click_through_pct, v_unique_inline_link_click_qty, v_unique_link_click_through_pct, v_unique_inline_link_click_through_pct;
  var v_version_count;
  var count = 0;

  try {
    var cur_plat_device = snowflake.execute(
      {sqlText: `SELECT CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID, POSITION_DC, PLATFORM_DC, DEVICE_DC,
      LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY,
      VIDEO_30_SEC_WATCHED_ACTION_ARRAY, VIDEO_P25_WATCHED_ACTION_ARRAY, VIDEO_P50_WATCHED_ACTION_ARRAY,
      VIDEO_P75_WATCHED_ACTION_ARRAY, WEBSITE_CLICK_THROUGH_ARRAY, CLICK_QTY, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT,
      PER_UNIQUE_CLICK_COST_AMT, PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT, AVERAGE_CPC_COST_AMT, AVERAGE_CPM_COST_AMT, PER_POINT_COST_AMT,
      CLICK_THROUGH_RATE_AMT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CLICK_THROUGH_PCT, INLINE_POST_ENGAGEMENT_QTY,
      OBJECTIVE_DC, REACH_QTY, SEND_QTY, UNIQUE_CLICK_QTY, UNIQUE_CLICK_THROUGH_PCT, UNIQUE_INLINE_LINK_CLICK_QTY, UNIQUE_LINK_CLICK_THROUGH_PCT,
      UNIQUE_INLINE_LINK_CLICK_THROUGH_PCT FROM ADDATAFUSION.FACEBOOK_ADS.VW_FS_ADS_PLATFORM_AND_DEVICE`}
    );

    while (cur_plat_device.next()) {

      v_cd2_ad_id= cur_plat_device.getColumnValue(1);
      v_cd2_adset_id= cur_plat_device.getColumnValue(2);
      v_cd2_campaign_id= cur_plat_device.getColumnValue(3);
      v_position_dc= cur_plat_device.getColumnValue(4);
      v_platform_dc= cur_plat_device.getColumnValue(5);
      v_device_dc = cur_plat_device.getColumnValue(6);
      v_load_dttm= cur_plat_device.getColumnValue(7);
      v_start_dttm= cur_plat_device.getColumnValue(8);
      v_stop_dttm= cur_plat_device.getColumnValue(9);
      v_action_array= cur_plat_device.getColumnValue(10);
      v_outbound_click_array= cur_plat_device.getColumnValue(11);
      v_unique_action_array= cur_plat_device.getColumnValue(12);
      v_unique_outbound_click_array= cur_plat_device.getColumnValue(13);
      v_video_30_sec_watched_action_array= cur_plat_device.getColumnValue(14);
      v_video_p25_watched_action_array= cur_plat_device.getColumnValue(15);
      v_video_p50_watched_action_array= cur_plat_device.getColumnValue(16);
      v_video_p75_watched_action_array= cur_plat_device.getColumnValue(17);
      v_website_click_through_array= cur_plat_device.getColumnValue(18);
      v_click_qty= cur_plat_device.getColumnValue(19);
      v_per_inline_link_click_cost_amt= cur_plat_device.getColumnValue(20);
      v_per_inline_post_engagement_cost_amt= cur_plat_device.getColumnValue(21);
      v_per_unique_click_cost_amt= cur_plat_device.getColumnValue(22);
      v_per_unique_inline_link_click_cost_amt= cur_plat_device.getColumnValue(23);
      v_average_cpc_cost_amt= cur_plat_device.getColumnValue(24);
      v_average_cpm_cost_amt= cur_plat_device.getColumnValue(25);
      v_per_point_cost_amt= cur_plat_device.getColumnValue(26);
      v_click_through_rate_amt= cur_plat_device.getColumnValue(27);
      v_frequency_qty= cur_plat_device.getColumnValue(28);
      v_impressions_qty= cur_plat_device.getColumnValue(29);
      v_inline_link_click_qty= cur_plat_device.getColumnValue(30);
      v_inline_link_click_through_pct= cur_plat_device.getColumnValue(31);
      v_inline_post_engagement_qty= cur_plat_device.getColumnValue(32);
      v_objective_dc= cur_plat_device.getColumnValue(33);
      v_reach_qty= cur_plat_device.getColumnValue(34);
      v_send_qty= cur_plat_device.getColumnValue(35);
      v_unique_click_qty= cur_plat_device.getColumnValue(36);
      v_unique_click_through_pct= cur_plat_device.getColumnValue(37);
      v_unique_inline_link_click_qty= cur_plat_device.getColumnValue(38);
      v_unique_link_click_through_pct= cur_plat_device.getColumnValue(39);
      v_unique_inline_link_click_through_pct= cur_plat_device.getColumnValue(40);

      

      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM ADDATAFUSION.DM_ADS.FS_ADS_PLATFORM_AND_DEVICE WHERE CD2_AD_ID=? AND CD2_ADSET_ID=? AND CD2_CAMPAIGN_ID=? AND PLATFORM_POSITION_DC=? AND PUBLISHER_PLATFORM_DC=? AND DEVICE_DC=? AND START_DTTM=? AND STOP_DTTM=?`,
        binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_position_dc, v_platform_dc, v_device_dc, v_start_dttm, v_stop_dttm]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count == 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_ADS_PLATFORM_AND_DEVICE (CD2_AD_ID, CD2_ADSET_ID,
          CD2_CAMPAIGN_ID, PLATFORM_POSITION_DC, PUBLISHER_PLATFORM_DC, DEVICE_DC, LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY,
          UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY, VIDEO_30_SEC_WATCHED_ACTION_ARRAY,
          VIDEO_P25_WATCHED_ACTION_ARRAY, VIDEO_P50_WATCHED_ACTION_ARRAY, VIDEO_P75_WATCHED_ACTION_ARRAY, WEBSITE_CLICK_THROUGH_ARRAY,
          CLICK_QTY, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT, PER_UNIQUE_CLICK_COST_AMT,
          PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT, AVERAGE_CPC_COST_AMT, AVERAGE_CPM_COST_AMT, CPP_COST_AMT, CTR_PCT, FREQUENCY_QTY, IMPRESSIONS_QTY,
          INLINE_LINK_CLICK_QTY, INLINE_LINK_CTR_PCT, INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_TXT, REACH_QTY, SPEND_AMT, UNIQUE_ACTION_QTY,
          UNIQUE_CLICK_QTY, UNIQUE_CTR_PCT, UNIQUE_INLINE_LINK_CLICK_QTY, UNIQUE_LINK_CLICK_CTR_QTY) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_position_dc, v_platform_dc, v_device_dc,
                  v_load_dttm, v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
                  v_video_30_sec_watched_action_array, v_video_p25_watched_action_array, v_video_p50_watched_action_array,
                  v_video_p75_watched_action_array, v_website_click_through_array, v_click_qty, v_per_inline_link_click_cost_amt,
                  v_per_inline_post_engagement_cost_amt, v_per_unique_click_cost_amt, v_per_unique_inline_link_click_cost_amt, v_average_cpc_cost_amt,
                  v_average_cpm_cost_amt, v_per_point_cost_amt, v_click_through_rate_amt, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty,
                  v_inline_link_click_through_pct, v_inline_post_engagement_qty, v_objective_dc, v_reach_qty, v_send_qty, v_unique_click_qty,
                  v_unique_click_through_pct, v_unique_inline_link_click_qty, v_unique_link_click_through_pct, v_unique_inline_link_click_through_pct]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Stored Procedure SP_LOAD_FS_ADS_PLATFORM_AND_DEVICE completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;