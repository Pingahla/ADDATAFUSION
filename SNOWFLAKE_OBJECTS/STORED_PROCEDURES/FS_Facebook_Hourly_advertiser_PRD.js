CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_ADS_HOURLY_ADVERTISER()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_time_range_dc, v_load_dttm,
  v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_website_click_through_array, v_account_currency_cd,
  v_buying_type_dc, v_click_qty, v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt, v_average_cpc_cost_amt,
  v_average_cpm_cost_amt, v_click_through_rate_amt, v_impressions_qty, v_inline_link_click_qty, v_inline_link_click_through_pct,
  v_inline_post_engagement_qty, v_objective_dc, v_send_qty;
  var v_version_count;
  var count = 0;

  try {
    var cur_hour_adv = snowflake.execute(
      {sqlText: `SELECT CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID, TIME_RANGE_DC, LOAD_DTTM,
      START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, WEBSITE_CLICK_THROUGH_ARRAY, ACCOUNT_CURRENCY_CD, BUYING_TYPE_DC,
      CLICK_QTY, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT, AVERAGE_CPC_COST_AMT, AVERAGE_CPM_COST_AMT,
      CLICK_THROUGH_RATE_AMT, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CLICK_THROUGH_PCT, INLINE_POST_ENGAGEMENT_QTY,
      OBJECTIVE_DC, SEND_QTY FROM ADDATAFUSION.FACEBOOK_ADS.VW_FS_ADS_HOURLY_ADVERTISER`}
    );

    while (cur_hour_adv.next()) {

      v_cd2_ad_id= cur_hour_adv.getColumnValue(1);
      v_cd2_adset_id= cur_hour_adv.getColumnValue(2);
      v_cd2_campaign_id= cur_hour_adv.getColumnValue(3);
      v_time_range_dc= cur_hour_adv.getColumnValue(4);
      v_load_dttm= cur_hour_adv.getColumnValue(5);
      v_start_dttm= cur_hour_adv.getColumnValue(6);
      v_stop_dttm= cur_hour_adv.getColumnValue(7);
      v_action_array= cur_hour_adv.getColumnValue(8);
      v_outbound_click_array= cur_hour_adv.getColumnValue(9);
      v_website_click_through_array= cur_hour_adv.getColumnValue(10);
      v_account_currency_cd= cur_hour_adv.getColumnValue(11);
      v_buying_type_dc= cur_hour_adv.getColumnValue(12);
      v_click_qty= cur_hour_adv.getColumnValue(13);
      v_per_inline_link_click_cost_amt= cur_hour_adv.getColumnValue(14);
      v_per_inline_post_engagement_cost_amt= cur_hour_adv.getColumnValue(15);
      v_average_cpc_cost_amt= cur_hour_adv.getColumnValue(16);
      v_average_cpm_cost_amt= cur_hour_adv.getColumnValue(17);
      v_click_through_rate_amt= cur_hour_adv.getColumnValue(18);
      v_impressions_qty= cur_hour_adv.getColumnValue(19);
      v_inline_link_click_qty= cur_hour_adv.getColumnValue(20);
      v_inline_link_click_through_pct= cur_hour_adv.getColumnValue(21);
      v_inline_post_engagement_qty= cur_hour_adv.getColumnValue(22);
      v_objective_dc= cur_hour_adv.getColumnValue(23);
      v_send_qty= cur_hour_adv.getColumnValue(24);

	  
      

      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM ADDATAFUSION.DM_ADS.FS_ADS_HOURLY_ADVERTISER WHERE CD2_AD_ID=? AND CD2_ADSET_ID=? AND CD2_CAMPAIGN_ID=? AND HOURLY_STATS_ADVERTISER_TIME_ZONE_DC=? AND START_DTTM=?`,
        binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_time_range_dc, v_start_dttm]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count == 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_ADS_HOURLY_ADVERTISER (CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID,
          HOURLY_STATS_ADVERTISER_TIME_ZONE_DC, LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, WEBSITE_CLICK_THROUGH_ARRAY,
          ACCOUNT_CURRENCY_CD, BUYING_TYPE_DC, CLICK_QTY, PER_INLINE_LINK_CLICK_COST_AMT, PER_INLINE_POST_ENGAGEMENT_COST_AMT, PER_CLICK_COST_AMT,
          PER_PER_THOUSAND_IMPRESSIONS_COST_AMT, CLICK_THROUGH_PCT, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY, INLINE_LINK_CLICK_THROUGH_PCT,
          INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_TXT, SPEND_AMT) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_time_range_dc, v_load_dttm,
                  v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_website_click_through_array, v_account_currency_cd,
                  v_buying_type_dc, v_click_qty, v_per_inline_link_click_cost_amt, v_per_inline_post_engagement_cost_amt, v_average_cpc_cost_amt,
                  v_average_cpm_cost_amt, v_click_through_rate_amt, v_impressions_qty, v_inline_link_click_qty, v_inline_link_click_through_pct,
                  v_inline_post_engagement_qty, v_objective_dc, v_send_qty]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_ADS_HOURLY_ADVERTISER completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;