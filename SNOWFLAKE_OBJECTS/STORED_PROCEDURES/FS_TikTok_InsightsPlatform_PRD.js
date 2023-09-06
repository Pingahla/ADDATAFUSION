CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_TIKTOK_ADS_INSIGHTS_PLATFORM()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_adgroup_id, v_adgroup_nm, v_advertiser_id, v_ad_id, v_ad_nm, v_ad_txt, v_campaign_id,
  v_campaign_nm, v_dpa_target_audience_type_dc, v_platform_dc, v_stat_dttm, v_impressions_qty,
  v_conversion_qty, v_convertion_pct, v_mobile_app_id, v_promotion_type_dc, v_tiktok_app_id,
  v_tiktok_app_nm, v_click_qty, v_cost_per_conversion_amt, v_cost_pre_result_amt, v_cost_per_click_amt,
  v_cost_per_thousand_impressions_amt, v_click_through_pct, v_real_time_conversion_qty,
  v_real_time_conversion_pct, v_real_time_cost_per_conversion_amt, v_real_time_cost_per_result_amt,
  v_real_time_result_qty, v_real_time_result_pct, v_result_qty, v_result_pct, v_spend_amt;
  var v_version_count;
  var count = 0;

  try {
    var cur_insghts_platform = snowflake.execute(
      {sqlText: `SELECT ADGROUP_ID, ADGROUP_NM, ADVERTISER_ID, AD_ID, AD_NM, AD_TXT, CAMPAIGN_ID,
      CAMPAIGN_NM, DPA_TARGET_AUDIENCE_TYPE_DC, PLATFORM_DC, STAT_DTTM, IMPRESSIONS_QTY,
      CONVERSION_QTY, CONVERTION_PCT, MOBILE_APP_ID, PROMOTION_TYPE_DC, TIKTOK_APP_ID, TIKTOK_APP_NM,
      CLICK_QTY, COST_PER_CONVERSION_AMT, COST_PRE_RESULT_AMT, COST_PER_CLICK_AMT,
      COST_PER_THOUSAND_IMPRESSIONS_AMT, CLICK_THROUGH_PCT, REAL_TIME_CONVERSION_QTY,
      REAL_TIME_CONVERSION_PCT, REAL_TIME_COST_PER_CONVERSION_AMT, REAL_TIME_COST_PER_RESULT_AMT,
      REAL_TIME_RESULT_QTY, REAL_TIME_RESULT_PCT, RESULT_QTY, RESULT_PCT, SPEND_AMT 
      FROM "ADDATAFUSION"."TIKTOK_ADS"."VW_FS_ADS_INSIGHTS_PLATFORM"`}
    );

    while (cur_insghts_platform.next()) {
          v_adgroup_id= cur_insghts_platform.getColumnValue(1);
          v_adgroup_nm= cur_insghts_platform.getColumnValue(2);
          v_advertiser_id= cur_insghts_platform.getColumnValue(3);
          v_ad_id= cur_insghts_platform.getColumnValue(4);
          v_ad_nm= cur_insghts_platform.getColumnValue(5);
          v_ad_txt= cur_insghts_platform.getColumnValue(6);
          v_campaign_id= cur_insghts_platform.getColumnValue(7);
          v_campaign_nm= cur_insghts_platform.getColumnValue(8);
          v_dpa_target_audience_type_dc= cur_insghts_platform.getColumnValue(9);
          v_platform_dc= cur_insghts_platform.getColumnValue(10);
          v_stat_dttm= cur_insghts_platform.getColumnValue(11);
          v_impressions_qty= cur_insghts_platform.getColumnValue(12);
          v_conversion_qty= cur_insghts_platform.getColumnValue(13);
          v_convertion_pct= cur_insghts_platform.getColumnValue(14);
          v_mobile_app_id= cur_insghts_platform.getColumnValue(15);
          v_promotion_type_dc= cur_insghts_platform.getColumnValue(16);
          v_tiktok_app_id= cur_insghts_platform.getColumnValue(17);
          v_tiktok_app_nm= cur_insghts_platform.getColumnValue(18);
          v_click_qty= cur_insghts_platform.getColumnValue(19);
          v_cost_per_conversion_amt= cur_insghts_platform.getColumnValue(20);
          v_cost_pre_result_amt= cur_insghts_platform.getColumnValue(21);
          v_cost_per_click_amt= cur_insghts_platform.getColumnValue(22);
          v_cost_per_thousand_impressions_amt= cur_insghts_platform.getColumnValue(23);
          v_click_through_pct= cur_insghts_platform.getColumnValue(24);
          v_real_time_conversion_qty= cur_insghts_platform.getColumnValue(25);
          v_real_time_conversion_pct= cur_insghts_platform.getColumnValue(26);
          v_real_time_cost_per_conversion_amt= cur_insghts_platform.getColumnValue(27);
          v_real_time_cost_per_result_amt= cur_insghts_platform.getColumnValue(28);
          v_real_time_result_qty= cur_insghts_platform.getColumnValue(29);
          v_real_time_result_pct= cur_insghts_platform.getColumnValue(30);
          v_result_qty= cur_insghts_platform.getColumnValue(31);
          v_result_pct= cur_insghts_platform.getColumnValue(32);
          v_spend_amt= cur_insghts_platform.getColumnValue(33);


      
      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM ADDATAFUSION.DM_ADS.FS_TIKTOK_ADS_INSIGHTS_BY_PLATFORM WHERE ADGROUP_ID=? AND
        ADVERTISER_ID=? AND AD_ID=? AND CAMPAIGN_ID=? AND PLATFORM_DC=? AND STAT_DTTM=?`,
        binds: [v_adgroup_id, v_advertiser_id, v_ad_id, v_campaign_id, v_platform_dc, v_stat_dttm]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_TIKTOK_ADS_INSIGHTS_BY_PLATFORM (ADGROUP_ID, ADGROUP_NM, ADVERTISER_ID,
          AD_ID, AD_NM, AD_TXT, CAMPAIGN_ID, CAMPAIGN_NM, DPA_TARGET_AUDIENCE_TYPE_DC, PLATFORM_DC, STAT_DTTM,
          IMPRESSIONS_QTY, CONVERSION_QTY, CONVERSION_PCT, MOBILE_APP_ID, PROMOTION_TYPE_DC, TIKTOK_APP_ID,
          TIKTOK_APP_NM, CLICK_QTY, COST_PER_CONVERSION_AMT, COST_PER_RESULT_AMT, COST_PER_CLICK_AMT,
          COST_PER_THOUSAND_IMPRESSIONS_AMT, CLICK_THROUGH_PCT, REAL_TIME_CONVERSION_QTY, REAL_TIME_CONVERSION_PCT,
          REAL_TIME_COST_PER_CONVERSION_AMT, REAL_TIME_COST_PER_RESULT_AMT, REAL_TIME_RESULT_QTY, REAL_TIME_RESULT_PCT,
          RESULT_QTY, RESULT_PCT, SPEND_AMT) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_adgroup_id, v_adgroup_nm, v_advertiser_id, v_ad_id, v_ad_nm, v_ad_txt, v_campaign_id,
                  v_campaign_nm, v_dpa_target_audience_type_dc, v_platform_dc, v_stat_dttm, v_impressions_qty,
                  v_conversion_qty, v_convertion_pct, v_mobile_app_id, v_promotion_type_dc, v_tiktok_app_id,
                  v_tiktok_app_nm, v_click_qty, v_cost_per_conversion_amt, v_cost_pre_result_amt, v_cost_per_click_amt,
                  v_cost_per_thousand_impressions_amt, v_click_through_pct, v_real_time_conversion_qty,
                  v_real_time_conversion_pct, v_real_time_cost_per_conversion_amt, v_real_time_cost_per_result_amt,
                  v_real_time_result_qty, v_real_time_result_pct, v_result_qty, v_result_pct, v_spend_amt]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_TIKTOK_ADS_INSIGHTS_PLATFORM completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;