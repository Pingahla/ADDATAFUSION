CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_CLICK()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_load_dttm, v_ad_group_id, v_ad_network_type_dc, v_device_dc, v_campaign_id, v_campaign_nm, v_click_view_gcl_id, v_customer_id,
  v_click_view_location_of_presence_array, v_campaign_status_dc, v_clicks_qty, v_click_type_dc, v_click_view_ad_group_ad_dc,
  v_click_view_campaign_location_target_dc, v_click_view_page_nr, v_slot_dc;
  var v_version_count;
  var count = 0;

  try {
    var cur_perf_click = snowflake.execute(
      {sqlText: `SELECT LOAD_DTTM, AD_GROUP_ID, AD_NETWORK_TYPE_DC, DEVICE_DC, CAMPAIGN_ID, CAMPAIGN_NM, CLICK_VIEW_GCL_ID,
      CUSTOMER_ID, CLICK_VIEW_LOCATION_OF_PRESENCE_ARRAY, CAMPAIGN_STATUS_DC, CLICKS_QTY, CLICK_TYPE_DC, CLICK_VIEW_AD_GROUP_AD_DC,
      CLICK_VIEW_CAMPAIGN_LOCATION_TARGET_DC, CLICK_VIEW_PAGE_NR, SLOT_DC FROM ADDATAFUSION.GOOGLE_ADS.VW_FS_GOOGLE_ADS_PERFORMANCE_CLICK`}
    );

    while (cur_perf_click.next()) {
          v_load_dttm= cur_perf_click.getColumnValue(1);
          v_ad_group_id= cur_perf_click.getColumnValue(2);
          v_ad_network_type_dc= cur_perf_click.getColumnValue(3);
          v_device_dc= cur_perf_click.getColumnValue(4);
          v_campaign_id= cur_perf_click.getColumnValue(5);
          v_campaign_nm= cur_perf_click.getColumnValue(6);
          v_click_view_gcl_id= cur_perf_click.getColumnValue(7);
          v_customer_id= cur_perf_click.getColumnValue(8);
          v_click_view_location_of_presence_array= cur_perf_click.getColumnValue(9);
          v_campaign_status_dc= cur_perf_click.getColumnValue(10);
          v_clicks_qty= cur_perf_click.getColumnValue(11);
          v_click_type_dc= cur_perf_click.getColumnValue(12);
          v_click_view_ad_group_ad_dc= cur_perf_click.getColumnValue(13);
          v_click_view_campaign_location_target_dc= cur_perf_click.getColumnValue(14);
          v_click_view_page_nr= cur_perf_click.getColumnValue(15);
          v_slot_dc= cur_perf_click.getColumnValue(16);
      

      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_CLICK WHERE LOAD_DTTM=? AND AD_GROUP_ID=? AND AD_NETWORK_TYPE_DC=?
        AND DEVICE_DC=? AND CAMPAIGN_ID=? AND CLICK_VIEW_GCL_ID=? AND CUSTOMER_ID=?`,
        binds: [v_load_dttm, v_ad_group_id, v_ad_network_type_dc, v_device_dc, v_campaign_id, v_click_view_gcl_id, v_customer_id]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_GOOGLE_ADS_PERFORMANCE_CLICK (LOAD_DTTM, AD_GROUP_ID, AD_NETWORK_TYPE_DC, DEVICE_DC, CAMPAIGN_ID,
          CAMPAIGN_NM, CLICK_VIEW_GCL_ID, CUSTOMER_ID, CLICK_VIEW_LOCATION_OF_PRESENCE_ARRAY, CAMPAIGN_STATUS_DC, CLICKS_QTY, CLICK_TYPE_DC,
          CLICK_VIEW_AD_GROUP_AD_DC, CLICK_VIEW_CAMPAIGN_LOCATION_TARGET_DC, CLICK_VIEW_PAGE_NR, SLOT_DC) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_load_dttm, v_ad_group_id, v_ad_network_type_dc, v_device_dc, v_campaign_id, v_campaign_nm, v_click_view_gcl_id, v_customer_id,
                  v_click_view_location_of_presence_array, v_campaign_status_dc, v_clicks_qty, v_click_type_dc, v_click_view_ad_group_ad_dc,
                  v_click_view_campaign_location_target_dc, v_click_view_page_nr, v_slot_dc]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_FS_GOOGLE_ADS_PERFORMANCE_CLICK completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
