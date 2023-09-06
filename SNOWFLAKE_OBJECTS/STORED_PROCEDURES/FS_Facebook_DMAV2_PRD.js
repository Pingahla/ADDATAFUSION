CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_ADS_DMA()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_cd2_ad_id, v_cd2_adset_id, v_cd2_campaign_id, v_dma_array,
  v_load_dttm, v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array,
  v_unique_outbound_click_array, v_website_click_through_array, v_click_qty, v_per_inline_link_click_average_cost_amt,
  v_per_inline_post_engagement_average_cost_amt, v_per_click_cost_amt, v_per_thousand_impressions_cost_amt,
  v_click_through_pct, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty, v_inline_post_engagement_qty,
  v_objective_dc, v_reach_qty, v_spend_amt, v_unique_click_qty;
  var v_version_count;
  var count = 0;

  try {
    var cur_dma = snowflake.execute(
      {sqlText: `SELECT CD2_AD_ID, CD2_ADSET_ID, CD2_CAMPAIGN_ID, DMA_ARRAY,
      LOAD_DTTM, START_DTTM, STOP_DTTM, ACTION_ARRAY, OUTBOUND_CLICK_ARRAY, UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY,
      WEBSITE_CLICK_THROUGH_ARRAY, CLICK_QTY, PER_INLINE_LINK_CLICK_AVERAGE_COST_AMT, PER_INLINE_POST_ENGAGEMENT_AVERAGE_COST_AMT,
      PER_CLICK_COST_AMT, PER_THOUSAND_IMPRESSIONS_COST_AMT, CLICK_THROUGH_PCT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY,
      INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_DC, REACH_QTY, SPEND_AMT, UNIQUE_CLICK_QTY FROM ADDATAFUSION.FACEBOOK_ADS.VW_FS_ADS_DMA`}
    );

    while (cur_dma.next()) {

      v_cd2_ad_id= cur_dma.getColumnValue(1);
      v_cd2_adset_id= cur_dma.getColumnValue(2);
      v_cd2_campaign_id= cur_dma.getColumnValue(3);
      v_dma_array= cur_dma.getColumnValue(4);
      v_load_dttm= cur_dma.getColumnValue(5);
      v_start_dttm= cur_dma.getColumnValue(6);
      v_stop_dttm= cur_dma.getColumnValue(7);
      v_action_array= cur_dma.getColumnValue(8);
      v_outbound_click_array= cur_dma.getColumnValue(9);
      v_unique_action_array= cur_dma.getColumnValue(10);
      v_unique_outbound_click_array= cur_dma.getColumnValue(11);
      v_website_click_through_array= cur_dma.getColumnValue(12);
      v_click_qty= cur_dma.getColumnValue(13);
      v_per_inline_link_click_average_cost_amt= cur_dma.getColumnValue(14);
      v_per_inline_post_engagement_average_cost_amt= cur_dma.getColumnValue(15);
      v_per_click_cost_amt= cur_dma.getColumnValue(16);
      v_per_thousand_impressions_cost_amt= cur_dma.getColumnValue(17);
      v_click_through_pct= cur_dma.getColumnValue(18);
      v_frequency_qty= cur_dma.getColumnValue(19);
      v_impressions_qty= cur_dma.getColumnValue(20);
      v_inline_link_click_qty= cur_dma.getColumnValue(21);
      v_inline_post_engagement_qty= cur_dma.getColumnValue(22);
      v_objective_dc= cur_dma.getColumnValue(23);
      v_reach_qty= cur_dma.getColumnValue(24);
      v_spend_amt= cur_dma.getColumnValue(25);
      v_unique_click_qty= cur_dma.getColumnValue(26);
	  
      

      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM ADDATAFUSION.DM_ADS.FS_ADS_DMA WHERE CD2_ADSET_ID=? AND CD2_AD_ID=? AND CD2_CAMPAIGN_ID=? AND DMA_ARRAY=? AND START_DTTM=?`,
        binds: [v_cd2_adset_id, v_cd2_ad_id, v_cd2_campaign_id, v_dma_array, v_start_dttm]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count == 0) { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_ADS_DMA (CD2_ADSET_ID, CD2_AD_ID, CD2_CAMPAIGN_ID, DMA_ARRAY,
          LOAD_DTTM, START_DTTM, STOP_DTTM, ACTIONS_ARRAY, OUTBOUND_CLICK_ARRAY, UNIQUE_ACTION_ARRAY, UNIQUE_OUTBOUND_CLICK_ARRAY,
          WEBSITE_CLICK_THROUGH_RATE_ARRAY, CLICK_QTY, PER_INLINE_LINK_CLICK_AVERAGE_COST_AMT, PER_INLINE_POST_ENGAGEMENT_AVERAGE_COST_AMT,
          PER_CLICK_COST_AMT, PER_THOUSAND_IMPRESSIONS_COST_AMT, CLICK_THROUGH_PCT, FREQUENCY_QTY, IMPRESSIONS_QTY, INLINE_LINK_CLICK_QTY,
          INLINE_POST_ENGAGEMENT_QTY, OBJECTIVE_DC, REACH_QTY, SPEND_AMT, UNIQUE_CLICK_QTY) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_cd2_adset_id, v_cd2_ad_id, v_cd2_campaign_id, v_dma_array,
                  v_load_dttm, v_start_dttm, v_stop_dttm, v_action_array, v_outbound_click_array, v_unique_action_array,
                  v_unique_outbound_click_array, v_website_click_through_array, v_click_qty, v_per_inline_link_click_average_cost_amt,
                  v_per_inline_post_engagement_average_cost_amt, v_per_click_cost_amt, v_per_thousand_impressions_cost_amt,
                  v_click_through_pct, v_frequency_qty, v_impressions_qty, v_inline_link_click_qty, v_inline_post_engagement_qty,
                  v_objective_dc, v_reach_qty, v_spend_amt, v_unique_click_qty]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_ADS_DMA completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;