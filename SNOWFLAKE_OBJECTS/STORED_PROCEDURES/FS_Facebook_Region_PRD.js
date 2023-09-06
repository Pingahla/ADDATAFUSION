CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_ADS_REGION()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
AS
$$

    var v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_region_dc, v_load_dttm,
    v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
    v_website_click_through_array, v_average_cpc_cost_amt, v_average_cpm_cost_amt, v_click_qty, v_per_point_cost_amt,
    v_click_through_rate_amt, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty, v_inline_link_click_through_pct,
    v_inline_post_engagement_qty, v_objective_dc, v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt,
    v_per_unique_click_cost_amt, v_per_unique_inline_link_click_cost_amt, v_reach_qty, v_send_qty, v_unique_click_qty,
    v_unique_click_through_pct, v_unique_inline_link_click_through_pct, v_unique_inline_link_click_qty,
    v_unique_link_click_through_pct;
    var v_version_count;
    var count = 0;
    
      try {
    var cur_fs_region = snowflake.execute(
      {sqlText: `SELECT CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID, REGION_DC, LOAD_DTTM,
      START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY,
      WEBSITE_CLICK_THROUGH_ARRAY, AVERAGE_CPC_COST_AMT, AVERAGE_CPM_COST_AMT, CLICK_QTY, PER_POINT_COST_AMT,
      CLICK_THROUGH_RATE_AMT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CLICK_THROUGH_PCT,
      INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_DC, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT,
      PER_UNIQUE_CLICK_COST_AMT, PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT, REACH_QTY, SEND_QTY, UNIQUE_CLICK_QTY,
      UNIQUE_CLICK_THROUGH_PCT, UNIQUE_INLINE_LINK_CLICK_THROUGH_PCT, UNIQUE_INLINE_LINK_CLICK_QTY,
      UNIQUE_LINK_CLICK_THROUGH_PCT FROM ADDATAFUSION.FACEBOOK_ADS.VW_FS_ADS_REGION`}
    );
    
    while (cur_fs_region.next()) {

      v_cd2_ad_id = cur_fs_region.getColumnValue(1);
      v_cd2_adset_id = cur_fs_region.getColumnValue(2);
      v_cd2_campaign_id = cur_fs_region.getColumnValue(3);
      v_region_dc = cur_fs_region.getColumnValue(4);
      v_load_dttm = cur_fs_region.getColumnValue(5);
      v_start_dttm = cur_fs_region.getColumnValue(6);
      v_stop_dttm = cur_fs_region.getColumnValue(7);
      v_action_array = cur_fs_region.getColumnValue(8);
      v_outbound_click_array = cur_fs_region.getColumnValue(9);
      v_unique_action_array = cur_fs_region.getColumnValue(10);
      v_unique_outbound_click_array = cur_fs_region.getColumnValue(11);
      v_website_click_through_array = cur_fs_region.getColumnValue(12);
      v_average_cpc_cost_amt = cur_fs_region.getColumnValue(13);
      v_average_cpm_cost_amt = cur_fs_region.getColumnValue(14);
      v_click_qty = cur_fs_region.getColumnValue(15);
      v_per_point_cost_amt = cur_fs_region.getColumnValue(16);
      v_click_through_rate_amt = cur_fs_region.getColumnValue(17);
      v_frequency_qty = cur_fs_region.getColumnValue(18);
      v_impressions_qty = cur_fs_region.getColumnValue(19);
      v_inline_link_click_qty = cur_fs_region.getColumnValue(20);
      v_inline_link_click_through_pct = cur_fs_region.getColumnValue(21);
      v_inline_post_engagement_qty = cur_fs_region.getColumnValue(22);
      v_objective_dc = cur_fs_region.getColumnValue(23);
      v_per_inline_link_click_cost_amt = cur_fs_region.getColumnValue(24);
      v_per_inline_post_engagement_cost_amt = cur_fs_region.getColumnValue(25);
      v_per_unique_click_cost_amt = cur_fs_region.getColumnValue(26);
      v_per_unique_inline_link_click_cost_amt = cur_fs_region.getColumnValue(27);
      v_reach_qty = cur_fs_region.getColumnValue(28);
      v_send_qty = cur_fs_region.getColumnValue(29);
      v_unique_click_qty = cur_fs_region.getColumnValue(30);
      v_unique_click_through_pct = cur_fs_region.getColumnValue(31);
      v_unique_inline_link_click_through_pct = cur_fs_region.getColumnValue(32);
      v_unique_inline_link_click_qty = cur_fs_region.getColumnValue(33);
      v_unique_link_click_through_pct = cur_fs_region.getColumnValue(34);

      
      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
          sqlText:  `SELECT COUNT(*) FROM ADDATAFUSION.DM_ADS.FS_ADS_REGION WHERE CD2_AD_ID=? AND CD2_ADSET_ID=? AND CD2_CAMPAIGN_ID=? AND REGION_DC=? AND START_DTTM=?`,

          binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_region_dc, v_start_dttm]
        });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }
      
      if (v_version_count == 0) { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
            sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_ADS_REGION (CD2_AD_ID, CD2_ADSET_ID,
            CD2_CAMPAIGN_ID, REGION_DC, LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY,
            UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY, WEBSITE_CTR_ARRAY, AVERAGE_CPC_COST_AMT, AVERAGE_CPM_COST_AMT,
            CLICK_QTY, CPP_COST_AMT, CTR_PCT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CTR_PCT,
            INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_DC, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT,
            PER_UNIQUE_CLICK_COST_AMT, PER_UNIQUE_INLINE_LINK_CLICK_COST_AMT, REACH_QTY, SPEND_AMT, UNIQUE_CLICK_QTY,
            UNIQUE_CTR_PCT, UNIQUE_INLINE_LINK_CLICK_CTR_QTY, UNIQUE_INLINE_LINK_CLICK_QTY, UNIQUE_LINK_CLICK_CTR_QTY) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?)`,
            binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_region_dc, v_load_dttm,
            v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array, v_unique_outbound_click_array,
            v_website_click_through_array, v_average_cpc_cost_amt, v_average_cpm_cost_amt, v_click_qty, v_per_point_cost_amt,
            v_click_through_rate_amt, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty, v_inline_link_click_through_pct,
            v_inline_post_engagement_qty, v_objective_dc, v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt,
            v_per_unique_click_cost_amt, v_per_unique_inline_link_click_cost_amt, v_reach_qty, v_send_qty, v_unique_click_qty,
            v_unique_click_through_pct, v_unique_inline_link_click_through_pct, v_unique_inline_link_click_qty,
            v_unique_link_click_through_pct]
          });
          stmt.execute();
          count++;

      }
   }
   
   return 'Region stored procedure completed succesfully. Records added: ' + count;
   } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
