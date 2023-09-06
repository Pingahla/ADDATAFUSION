CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_CAMPAIGN()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_load_dttm, v_ad_network_type_dc, v_customer_id, v_campaign_id, v_absolute_top_impression_pct,
  v_active_view_cost_per_mile_amt, v_active_view_click_through_pct, v_active_view_impressions_qty,
  v_active_view_measurability_pct, v_active_view_measurable_cost_micros_qty, v_active_view_measurable_impressions_qty,
  v_active_view_viewability_pct, v_all_conversions_pct, v_all_conversions_from_interactions_pct, v_all_conversions_value_pct,
  v_average_cost_amt, v_average_cost_per_click_amt, v_average_cost_per_thousand_impressions_amt, v_bidding_strategy_nm,
  v_campaign_advertising_channel_sub_type_dc, v_campaign_advertising_channel_type_dc, v_campaign_base_campaign_dc,
  v_campaign_bidding_strategy_dc, v_campaign_bidding_strategy_type_dc, v_campaign_budget_amount_micros_qty,
  v_campaign_budget_explicitly_shared_ind, v_campaign_budget_has_recommended_budget_ind, v_campaign_budget_period_dc,
  v_campaign_campaign_budget_dc, v_campaign_end_dttm, v_campaign_experiment_type_dc, v_campaign_nm, v_campaign_serving_status_dc,
  v_campaign_start_dttm, v_campaign_status_dc, v_clicks_qty, v_content_budget_lost_impression_share_pct,
  v_content_impression_share_pct, v_content_rank_lost_impression_share_pct, v_conversions_pct, v_conversions_from_interactions_pct,
  v_conversions_value_amt, v_cost_micros_qty, v_cross_device_conversions_pct, v_click_through_pct, v_current_model_attributed_conversions_pct,
  v_current_model_attributed_conversions_value_pct, v_customer_currency_cd, v_customer_time_zone_dc, v_engagements_qty, v_gmail_forwards_qty,
  v_gmail_saves_qty, v_gmail_secondary_clicks_qty, v_impressions_qty, v_interactions_qty, v_interaction_event_types_dc, v_interaction_pct,
  v_invalid_clicks_qty, v_invalid_click_pct, v_phone_calls_qty, v_phone_impressions_qty, v_phone_through_pct, v_search_click_share_pct,
  v_top_impression_pct, v_video_quartile_p025_pct, v_video_quartile_p050_pct, v_video_quartile_p075_pct, v_video_quartile_p100_pct,
  v_video_views_qty, v_view_through_conversions_qty;
  var v_version_count;
  var count = 0;

  try {
    var cur_perf_camp = snowflake.execute(
      {sqlText: `SELECT LOAD_DTTM, AD_NETWORK_TYPE_DC, CUSTOMER_ID, CAMPAIGN_ID, ABSOLUTE_TOP_IMPRESSION_PCT, ACTIVE_VIEW_COST_PER_MILE_AMT,
      ACTIVE_VIEW_CLICK_THROUGH_PCT, ACTIVE_VIEW_IMPRESSIONS_QTY, ACTIVE_VIEW_MEASURABILITY_PCT, ACTIVE_VIEW_MEASURABLE_COST_MICROS_QTY,
      ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_QTY, ACTIVE_VIEW_VIEWABILITY_PCT, ALL_CONVERSIONS_PCT, ALL_CONVERSIONS_FROM_INTERACTIONS_PCT,
      ALL_CONVERSIONS_VALUE_PCT, AVERAGE_COST_AMT, AVERAGE_COST_PER_CLICK_AMT, AVERAGE_COST_PER_THOUSAND_IMPRESSIONS_AMT, BIDDING_STRATEGY_NM,
      CAMPAIGN_ADVERTISING_CHANNEL_SUB_TYPE_DC, CAMPAIGN_ADVERTISING_CHANNEL_TYPE_DC, CAMPAIGN_BASE_CAMPAIGN_DC, CAMPAIGN_BIDDING_STRATEGY_DC,
      CAMPAIGN_BIDDING_STRATEGY_TYPE_DC, CAMPAIGN_BUDGET_AMOUNT_MICROS_QTY, CAMPAIGN_BUDGET_EXPLICITLY_SHARED_IND,
      CAMPAIGN_BUDGET_HAS_RECOMMENDED_BUDGET_IND, CAMPAIGN_BUDGET_PERIOD_DC, CAMPAIGN_CAMPAIGN_BUDGET_DC, CAMPAIGN_END_DTTM,
      CAMPAIGN_EXPERIMENT_TYPE_DC, CAMPAIGN_NM, CAMPAIGN_SERVING_STATUS_DC, CAMPAIGN_START_DTTM, CAMPAIGN_STATUS_DC, CLICKS_QTY,
      CONTENT_BUDGET_LOST_IMPRESSION_SHARE_PCT, CONTENT_IMPRESSION_SHARE_PCT, CONTENT_RANK_LOST_IMPRESSION_SHARE_PCT, CONVERSIONS_PCT,
      CONVERSIONS_FROM_INTERACTIONS_PCT, CONVERSIONS_VALUE_AMT, COST_MICROS_QTY, CROSS_DEVICE_CONVERSIONS_PCT, CLICK_THROUGH_PCT,
      CURRENT_MODEL_ATTRIBUTED_CONVERSIONS_PCT, CURRENT_MODEL_ATTRIBUTED_CONVERSIONS_VALUE_PCT, CUSTOMER_CURRENCY_CD, CUSTOMER_TIME_ZONE_DC,
      ENGAGEMENTS_QTY, GMAIL_FORWARDS_QTY, GMAIL_SAVES_QTY, GMAIL_SECONDARY_CLICKS_QTY, IMPRESSIONS_QTY, INTERACTIONS_QTY,
      INTERACTION_EVENT_TYPES_DC, INTERACTION_PCT, INVALID_CLICKS_QTY, INVALID_CLICK_PCT, PHONE_CALLS_QTY, PHONE_IMPRESSIONS_QTY,
      PHONE_THROUGH_PCT, SEARCH_CLICK_SHARE_PCT, TOP_IMPRESSION_PCT, VIDEO_QUARTILE_P025_PCT, VIDEO_QUARTILE_P050_PCT,
      VIDEO_QUARTILE_P075_PCT, VIDEO_QUARTILE_P100_PCT, VIDEO_VIEWS_QTY, VIEW_THROUGH_CONVERSIONS_QTY FROM ADDATAFUSION.GOOGLE_ADS.VW_FS_GOOGLE_ADS_PERFORMANCE_CAMPAIGN`}
    );

    while (cur_perf_camp.next()) {
      v_load_dttm = cur_perf_camp.getColumnValue(1);
      v_ad_network_type_dc= cur_perf_camp.getColumnValue(2);
      v_customer_id= cur_perf_camp.getColumnValue(3);
      v_campaign_id= cur_perf_camp.getColumnValue(4);
      v_absolute_top_impression_pct= cur_perf_camp.getColumnValue(5);
      v_active_view_cost_per_mile_amt= cur_perf_camp.getColumnValue(6);
      v_active_view_click_through_pct= cur_perf_camp.getColumnValue(7);
      v_active_view_impressions_qty= cur_perf_camp.getColumnValue(8);
      v_active_view_measurability_pct= cur_perf_camp.getColumnValue(9);
      v_active_view_measurable_cost_micros_qty= cur_perf_camp.getColumnValue(10);
      v_active_view_measurable_impressions_qty= cur_perf_camp.getColumnValue(11);
      v_active_view_viewability_pct= cur_perf_camp.getColumnValue(12);
      v_all_conversions_pct= cur_perf_camp.getColumnValue(13);
      v_all_conversions_from_interactions_pct= cur_perf_camp.getColumnValue(14);
      v_all_conversions_value_pct= cur_perf_camp.getColumnValue(15);
      v_average_cost_amt= cur_perf_camp.getColumnValue(16);
      v_average_cost_per_click_amt= cur_perf_camp.getColumnValue(17);
      v_average_cost_per_thousand_impressions_amt= cur_perf_camp.getColumnValue(18);
      v_bidding_strategy_nm= cur_perf_camp.getColumnValue(19);
      v_campaign_advertising_channel_sub_type_dc= cur_perf_camp.getColumnValue(20);
      v_campaign_advertising_channel_type_dc= cur_perf_camp.getColumnValue(21);
      v_campaign_base_campaign_dc= cur_perf_camp.getColumnValue(22);
      v_campaign_bidding_strategy_dc= cur_perf_camp.getColumnValue(23);
      v_campaign_bidding_strategy_type_dc= cur_perf_camp.getColumnValue(24);
      v_campaign_budget_amount_micros_qty= cur_perf_camp.getColumnValue(25);
      v_campaign_budget_explicitly_shared_ind= cur_perf_camp.getColumnValue(26);
      v_campaign_budget_has_recommended_budget_ind= cur_perf_camp.getColumnValue(27);
      v_campaign_budget_period_dc= cur_perf_camp.getColumnValue(28);
      v_campaign_campaign_budget_dc= cur_perf_camp.getColumnValue(29);
      v_campaign_end_dttm= cur_perf_camp.getColumnValue(30);
      v_campaign_experiment_type_dc= cur_perf_camp.getColumnValue(31);
      v_campaign_nm= cur_perf_camp.getColumnValue(32);
      v_campaign_serving_status_dc= cur_perf_camp.getColumnValue(33);
      v_campaign_start_dttm= cur_perf_camp.getColumnValue(34);
      v_campaign_status_dc= cur_perf_camp.getColumnValue(35);
      v_clicks_qty= cur_perf_camp.getColumnValue(36);
      v_content_budget_lost_impression_share_pct= cur_perf_camp.getColumnValue(37);
      v_content_impression_share_pct= cur_perf_camp.getColumnValue(38);
      v_content_rank_lost_impression_share_pct= cur_perf_camp.getColumnValue(39);
      v_conversions_pct= cur_perf_camp.getColumnValue(40);
      v_conversions_from_interactions_pct= cur_perf_camp.getColumnValue(41);
      v_conversions_value_amt= cur_perf_camp.getColumnValue(42);
      v_cost_micros_qty= cur_perf_camp.getColumnValue(43);
      v_cross_device_conversions_pct= cur_perf_camp.getColumnValue(44);
      v_click_through_pct= cur_perf_camp.getColumnValue(45);
      v_current_model_attributed_conversions_pct= cur_perf_camp.getColumnValue(46);
      v_current_model_attributed_conversions_value_pct= cur_perf_camp.getColumnValue(47);
      v_customer_currency_cd= cur_perf_camp.getColumnValue(48);
      v_customer_time_zone_dc= cur_perf_camp.getColumnValue(49);
      v_engagements_qty= cur_perf_camp.getColumnValue(50);
      v_gmail_forwards_qty= cur_perf_camp.getColumnValue(51);
      v_gmail_saves_qty= cur_perf_camp.getColumnValue(52);
      v_gmail_secondary_clicks_qty= cur_perf_camp.getColumnValue(53);
      v_impressions_qty= cur_perf_camp.getColumnValue(54);
      v_interactions_qty= cur_perf_camp.getColumnValue(55);
      v_interaction_event_types_dc= cur_perf_camp.getColumnValue(56);
      v_interaction_pct= cur_perf_camp.getColumnValue(57);
      v_invalid_clicks_qty= cur_perf_camp.getColumnValue(58);
      v_invalid_click_pct= cur_perf_camp.getColumnValue(59);
      v_phone_calls_qty= cur_perf_camp.getColumnValue(60);
      v_phone_impressions_qty= cur_perf_camp.getColumnValue(61);
      v_phone_through_pct= cur_perf_camp.getColumnValue(62);
      v_search_click_share_pct= cur_perf_camp.getColumnValue(63);
      v_top_impression_pct= cur_perf_camp.getColumnValue(64);
      v_video_quartile_p025_pct= cur_perf_camp.getColumnValue(65);
      v_video_quartile_p050_pct= cur_perf_camp.getColumnValue(66);
      v_video_quartile_p075_pct= cur_perf_camp.getColumnValue(67);
      v_video_quartile_p100_pct= cur_perf_camp.getColumnValue(68);
      v_video_views_qty= cur_perf_camp.getColumnValue(69);
      v_view_through_conversions_qty= cur_perf_camp.getColumnValue(70);
      

      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_CAMPAIGN WHERE LOAD_DTTM=? AND
        AD_NETWORK_TYPE_DC=? AND CUSTOMER_ID=? AND CAMPAIGN_ID=?`,
        binds: [v_load_dttm, v_ad_network_type_dc, v_customer_id, v_campaign_id]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_CAMPAIGN (LOAD_DTTM, AD_NETWORK_TYPE_DC, CUSTOMER_ID, CAMPAIGN_ID, ABSOLUTE_TOP_IMPRESSION_PCT,
          ACTIVE_VIEW_COST_PER_MILE_AMT, ACTIVE_VIEW_CLICK_THROUGH_PCT, ACTIVE_VIEW_IMPRESSIONS_QTY, ACTIVE_VIEW_MEASURABILITY_PCT,
          ACTIVE_VIEW_MEASURABLE_COST_MICROS_QTY, ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_QTY, ACTIVE_VIEW_VIEWABILITY_PCT, ALL_CONVERSIONS_PCT,
          ALL_CONVERSIONS_FROM_INTERACTIONS_PCT, ALL_CONVERSIONS_VALUE_PCT, AVERAGE_COST_AMT, AVERAGE_COST_PER_CLICK_AMT, 
          AVERAGE_COST_PER_THOUSAND_IMPRESSIONS_AMT, BIDDING_STRATEGY_NM, CAMPAIGN_ADVERTISING_CHANNEL_SUB_TYPE_DC, CAMPAIGN_ADVERTISING_CHANNEL_TYPE_DC,
          CAMPAIGN_BASE_CAMPAIGN_DC, CAMPAIGN_BIDDING_STRATEGY_DC, CAMPAIGN_BIDDING_STRATEGY_TYPE_DC, CAMPAIGN_BUDGET_AMOUNT_MICROS_QTY,
          CAMPAIGN_BUDGET_EXPLICITLY_SHARED_IND, CAMPAIGN_BUDGET_HAS_RECOMMENDED_BUDGET_IND, CAMPAIGN_BUDGET_PERIOD_DC, CAMPAIGN_CAMPAIGN_BUDGET_DC,
          CAMPAIGN_END_DTTM, CAMPAIGN_EXPERIMENT_TYPE_DC, CAMPAIGN_NM, CAMPAIGN_SERVING_STATUS_DC, CAMPAIGN_START_DTTM, CAMPAIGN_STATUS_DC, CLICKS_QTY,
          CONTENT_BUDGET_LOST_IMPRESSION_SHARE_PCT, CONTENT_IMPRESSION_SHARE_PCT, CONTENT_RANK_LOST_IMPRESSION_SHARE_PCT, CONVERSIONS_PCT,
          CONVERSIONS_FROM_INTERACTIONS_PCT, CONVERSIONS_VALUE_AMT, COST_MICROS_QTY, CROSS_DEVICE_CONVERSIONS_PCT, CLICK_THROUGH_PCT,
          CURRENT_MODEL_ATTRIBUTED_CONVERSIONS_PCT, CURRENT_MODEL_ATTRIBUTED_CONVERSIONS_VALUE_PCT, CUSTOMER_CURRENCY_CD, CUSTOMER_TIME_ZONE_DC,
          ENGAGEMENTS_QTY, GMAIL_FORWARDS_QTY, GMAIL_SAVES_QTY, GMAIL_SECONDARY_CLICKS_QTY, IMPRESSIONS_QTY, INTERACTIONS_QTY, INTERACTION_EVENT_TYPES_DC,
          INTERACTION_PCT, INVALID_CLICKS_QTY, INVALID_CLICK_PCT, PHONE_CALLS_QTY, PHONE_IMPRESSIONS_QTY, PHONE_THROUGH_PCT, SEARCH_CLICK_SHARE_PCT,
          TOP_IMPRESSION_PCT, VIDEO_QUARTILE_P025_PCT, VIDEO_QUARTILE_P050_PCT, VIDEO_QUARTILE_P075_PCT, VIDEO_QUARTILE_P100_PCT, VIDEO_VIEWS_QTY,
          VIEW_THROUGH_CONVERSIONS_QTY) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_load_dttm, v_ad_network_type_dc, v_customer_id, v_campaign_id, v_absolute_top_impression_pct,
                  v_active_view_cost_per_mile_amt, v_active_view_click_through_pct, v_active_view_impressions_qty,
                  v_active_view_measurability_pct, v_active_view_measurable_cost_micros_qty, v_active_view_measurable_impressions_qty,
                  v_active_view_viewability_pct, v_all_conversions_pct, v_all_conversions_from_interactions_pct, v_all_conversions_value_pct,
                  v_average_cost_amt, v_average_cost_per_click_amt, v_average_cost_per_thousand_impressions_amt, v_bidding_strategy_nm,
                  v_campaign_advertising_channel_sub_type_dc, v_campaign_advertising_channel_type_dc, v_campaign_base_campaign_dc,
                  v_campaign_bidding_strategy_dc, v_campaign_bidding_strategy_type_dc, v_campaign_budget_amount_micros_qty,
                  v_campaign_budget_explicitly_shared_ind, v_campaign_budget_has_recommended_budget_ind, v_campaign_budget_period_dc,
                  v_campaign_campaign_budget_dc, v_campaign_end_dttm, v_campaign_experiment_type_dc, v_campaign_nm, v_campaign_serving_status_dc,
                  v_campaign_start_dttm, v_campaign_status_dc, v_clicks_qty, v_content_budget_lost_impression_share_pct,
                  v_content_impression_share_pct, v_content_rank_lost_impression_share_pct, v_conversions_pct, v_conversions_from_interactions_pct,
                  v_conversions_value_amt, v_cost_micros_qty, v_cross_device_conversions_pct, v_click_through_pct, v_current_model_attributed_conversions_pct,
                  v_current_model_attributed_conversions_value_pct, v_customer_currency_cd, v_customer_time_zone_dc, v_engagements_qty, v_gmail_forwards_qty,
                  v_gmail_saves_qty, v_gmail_secondary_clicks_qty, v_impressions_qty, v_interactions_qty, v_interaction_event_types_dc, v_interaction_pct,
                  v_invalid_clicks_qty, v_invalid_click_pct, v_phone_calls_qty, v_phone_impressions_qty, v_phone_through_pct, v_search_click_share_pct,
                  v_top_impression_pct, v_video_quartile_p025_pct, v_video_quartile_p050_pct, v_video_quartile_p075_pct, v_video_quartile_p100_pct,
                  v_video_views_qty, v_view_through_conversions_qty]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_CAMPAIGN completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
