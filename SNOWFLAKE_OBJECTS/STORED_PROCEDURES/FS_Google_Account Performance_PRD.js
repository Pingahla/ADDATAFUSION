CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_ACCOUNT()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_load_dttm, v_ad_network_type_dc, v_customer_id, v_device_dc, v_active_view_cost_per_mile_amt,
  v_active_view_click_through_pct, v_active_view_impressions_qty, v_active_view_measurability_pct,
  v_active_view_measurable_cost_micros_qty, v_active_view_measurable_impressions_qty, v_active_view_viewability_pct,
  v_all_conversions_pct, v_all_conversions_from_interactions_pct, v_all_conversions_value_amt, v_average_cost_amt,
  v_average_cost_per_click_amt, v_average_cost_per_mile_amt, v_clicks_qty, v_content_budget_lost_impression_share_pct,
  v_content_impression_share_pct, v_content_rank_lost_impression_share_pct, v_conversions_pct,
  v_conversions_from_interactions_pct, v_conversions_value_pct, v_cost_micros_qty, v_cross_device_conversions_pct,
  v_click_through_pct, v_customer_auto_tagging_enabled_ind, v_customer_currency_cd, v_customer_manager_ind,
  v_customer_test_account_ind, v_customer_time_zone_dc, v_engagements_qty, v_impressions_qty, v_interactions_qty,
  v_interaction_event_types_dc, v_interaction_pct, v_invalid_clicks_qty, v_invalid_click_pct, v_video_views_qty,
  v_view_through_conversions_qty;
  var v_version_count;
  var count = 0;
  
    try {
    var cur_performance_account = snowflake.execute(
      {sqlText: `SELECT LOAD_DTTM, AD_NETWORK_TYPE_DC, CUSTOMER_ID, DEVICE_DC, ACTIVE_VIEW_COST_PER_MILE_AMT,
      ACTIVE_VIEW_CLICK_THROUGH_PCT, ACTIVE_VIEW_IMPRESSIONS_QTY, ACTIVE_VIEW_MEASURABILITY_PCT,
      ACTIVE_VIEW_MEASURABLE_COST_MICROS_QTY, ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_QTY, ACTIVE_VIEW_VIEWABILITY_PCT,
      ALL_CONVERSIONS_PCT, ALL_CONVERSIONS_FROM_INTERACTIONS_PCT, ALL_CONVERSIONS_VALUE_AMT, AVERAGE_COST_AMT,
      AVERAGE_COST_PER_CLICK_AMT, AVERAGE_COST_PER_MILE_AMT, CLICKS_QTY, CONTENT_BUDGET_LOST_IMPRESSION_SHARE_PCT,
      CONTENT_IMPRESSION_SHARE_PCT, CONTENT_RANK_LOST_IMPRESSION_SHARE_PCT, CONVERSIONS_PCT, CONVERSIONS_FROM_INTERACTIONS_PCT,
      CONVERSIONS_VALUE_PCT, COST_MICROS_QTY, CROSS_DEVICE_CONVERSIONS_PCT, CLICK_THROUGH_PCT, CUSTOMER_AUTO_TAGGING_ENABLED_IND,
      CUSTOMER_CURRENCY_CD, CUSTOMER_MANAGER_IND, CUSTOMER_TEST_ACCOUNT_IND, CUSTOMER_TIME_ZONE_DC, ENGAGEMENTS_QTY,
      IMPRESSIONS_QTY, INTERACTIONS_QTY, INTERACTION_EVENT_TYPES_DC, INTERACTION_PCT, INVALID_CLICKS_QTY, INVALID_CLICK_PCT,
      VIDEO_VIEWS_QTY, VIEW_THROUGH_CONVERSIONS_QTY FROM ADDATAFUSION.GOOGLE_ADS.VW_FS_GOOGLE_ADS_PERFORMANCE_ACCOUNT`}
    );
    
    while (cur_performance_account.next()) {
      v_load_dttm=cur_performance_account.getColumnValue(1);
      v_ad_network_type_dc=cur_performance_account.getColumnValue(2);
      v_customer_id=cur_performance_account.getColumnValue(3);
      v_device_dc=cur_performance_account.getColumnValue(4);
      v_active_view_cost_per_mile_amt=cur_performance_account.getColumnValue(5);
      v_active_view_click_through_pct=cur_performance_account.getColumnValue(6);
      v_active_view_impressions_qty=cur_performance_account.getColumnValue(7);
      v_active_view_measurability_pct=cur_performance_account.getColumnValue(8);
      v_active_view_measurable_cost_micros_qty=cur_performance_account.getColumnValue(9);
      v_active_view_measurable_impressions_qty=cur_performance_account.getColumnValue(10);
      v_active_view_viewability_pct=cur_performance_account.getColumnValue(11);
      v_all_conversions_pct=cur_performance_account.getColumnValue(12);
      v_all_conversions_from_interactions_pct=cur_performance_account.getColumnValue(13);
      v_all_conversions_value_amt=cur_performance_account.getColumnValue(14);
      v_average_cost_amt=cur_performance_account.getColumnValue(15);
      v_average_cost_per_click_amt=cur_performance_account.getColumnValue(16);
      v_average_cost_per_mile_amt=cur_performance_account.getColumnValue(17);
      v_clicks_qty=cur_performance_account.getColumnValue(18);
      v_content_budget_lost_impression_share_pct=cur_performance_account.getColumnValue(19);
      v_content_impression_share_pct=cur_performance_account.getColumnValue(20);
      v_content_rank_lost_impression_share_pct=cur_performance_account.getColumnValue(21);
      v_conversions_pct=cur_performance_account.getColumnValue(22);
      v_conversions_from_interactions_pct=cur_performance_account.getColumnValue(23);
      v_conversions_value_pct=cur_performance_account.getColumnValue(24);
      v_cost_micros_qty=cur_performance_account.getColumnValue(25);
      v_cross_device_conversions_pct=cur_performance_account.getColumnValue(26);
      v_click_through_pct=cur_performance_account.getColumnValue(27);
      v_customer_auto_tagging_enabled_ind=cur_performance_account.getColumnValue(28);
      v_customer_currency_cd=cur_performance_account.getColumnValue(29);
      v_customer_manager_ind=cur_performance_account.getColumnValue(30);
      v_customer_test_account_ind=cur_performance_account.getColumnValue(31);
      v_customer_time_zone_dc=cur_performance_account.getColumnValue(32);
      v_engagements_qty=cur_performance_account.getColumnValue(33);
      v_impressions_qty=cur_performance_account.getColumnValue(34);
      v_interactions_qty=cur_performance_account.getColumnValue(35);
      v_interaction_event_types_dc=cur_performance_account.getColumnValue(36);
      v_interaction_pct=cur_performance_account.getColumnValue(37);
      v_invalid_clicks_qty=cur_performance_account.getColumnValue(38);
      v_invalid_click_pct=cur_performance_account.getColumnValue(39);
      v_video_views_qty=cur_performance_account.getColumnValue(40);
      v_view_through_conversions_qty=cur_performance_account.getColumnValue(41);
      
      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_ACCOUNT WHERE LOAD_DTTM=? AND
        AD_NETWORK_TYPE_DC=? AND CUSTOMER_ID=? AND DEVICE_DC=?`,
        binds: [v_load_dttm, v_ad_network_type_dc, v_customer_id, v_device_dc]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }
      
      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
            sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_ACCOUNT (LOAD_DTTM, AD_NETWORK_TYPE_DC, CUSTOMER_ID,
            DEVICE_DC, ACTIVE_VIEW_COST_PER_MILE_AMT, ACTIVE_VIEW_CLICK_THROUGH_PCT, ACTIVE_VIEW_IMPRESSIONS_QTY,
            ACTIVE_VIEW_MEASURABILITY_PCT, ACTIVE_VIEW_MEASURABLE_COST_MICROS_QTY, ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_QTY,
            ACTIVE_VIEW_VIEWABILITY_PCT, ALL_CONVERSIONS_PCT, ALL_CONVERSIONS_FROM_INTERACTIONS_PCT, ALL_CONVERSIONS_VALUE_AMT,
            AVERAGE_COST_AMT, AVERAGE_COST_PER_CLICK_AMT, AVERAGE_COST_PER_MILE_AMT, CLICKS_QTY,
            CONTENT_BUDGET_LOST_IMPRESSION_SHARE_PCT, CONTENT_IMPRESSION_SHARE_PCT, CONTENT_RANK_LOST_IMPRESSION_SHARE_PCT,
            CONVERSIONS_PCT, CONVERSIONS_FROM_INTERACTIONS_PCT, CONVERSIONS_VALUE_PCT, COST_MICROS_QTY,
            CROSS_DEVICE_CONVERSIONS_PCT, CLICK_THROUGH_PCT, CUSTOMER_AUTO_TAGGING_ENABLED_IND, CUSTOMER_CURRENCY_CD,
            CUSTOMER_MANAGER_IND, CUSTOMER_TEST_ACCOUNT_IND, CUSTOMER_TIME_ZONE_DC, ENGAGEMENTS_QTY, IMPRESSIONS_QTY,
            INTERACTIONS_QTY, INTERACTION_EVENT_TYPES_DC, INTERACTION_PCT, INVALID_CLICKS_QTY, INVALID_CLICK_PCT,
            VIDEO_VIEWS_QTY, VIEW_THROUGH_CONVERSIONS_QTY)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?)`,
        binds: [v_load_dttm, v_ad_network_type_dc, v_customer_id, v_device_dc, v_active_view_cost_per_mile_amt,
                v_active_view_click_through_pct, v_active_view_impressions_qty, v_active_view_measurability_pct,
                v_active_view_measurable_cost_micros_qty, v_active_view_measurable_impressions_qty, v_active_view_viewability_pct,
                v_all_conversions_pct, v_all_conversions_from_interactions_pct, v_all_conversions_value_amt, v_average_cost_amt,
                v_average_cost_per_click_amt, v_average_cost_per_mile_amt, v_clicks_qty, v_content_budget_lost_impression_share_pct,
                v_content_impression_share_pct, v_content_rank_lost_impression_share_pct, v_conversions_pct,
                v_conversions_from_interactions_pct, v_conversions_value_pct, v_cost_micros_qty, v_cross_device_conversions_pct,
                v_click_through_pct, v_customer_auto_tagging_enabled_ind, v_customer_currency_cd, v_customer_manager_ind,
                v_customer_test_account_ind, v_customer_time_zone_dc, v_engagements_qty, v_impressions_qty, v_interactions_qty,
                v_interaction_event_types_dc, v_interaction_pct, v_invalid_clicks_qty, v_invalid_click_pct, v_video_views_qty,
                v_view_through_conversions_qty]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_ACCOUNT() completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;



  
  
  
  
  